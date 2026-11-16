import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import { activeSilos, ringSilos } from "@thresh/core/membership";
import type { GrainDirectoryPartition } from "@thresh/directory/grain-directory-partition";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import {
  TestCluster,
  type TestClusterOptions,
  type TestSiloHandle,
} from "@thresh/testing/test-cluster";

/**
 * `TestCluster` on the production membership path: every silo runs its own
 * `KubernetesMembership` over its own watch of one fake EndpointSlice API, and the test drives
 * the API the way Kubernetes would. Mirrors the hand-composed scenarios in
 * `packages/hosting/src/kubernetes-membership-view.test.ts`.
 */
const Counter = defineGrain("K8sHarnessCounter", () => {
  let n = 0;
  return { inc: async (): Promise<number> => ++n };
});

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(options: Partial<TestClusterOptions> = {}): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    membership: "kubernetes",
    grains: [Counter],
    reminders: false,
    callTimeout: { seconds: 2 },
    ...options,
  });
  clusters.push(cluster);
  return cluster;
}

const names = (cluster: TestCluster, silo: TestSiloHandle): string[] =>
  activeSilos(cluster.viewOf(silo).membership)
    .map((s) => s.podName)
    .sort();

const ringNames = (cluster: TestCluster, silo: TestSiloHandle): string[] =>
  ringSilos(cluster.viewOf(silo).membership)
    .map((s) => s.podName)
    .sort();

/** The silo's directory partition (introspection only). */
const partitionOf = (silo: TestSiloHandle): GrainDirectoryPartition =>
  (silo.host as unknown as { parts: { node: { partition: GrainDirectoryPartition } } }).parts.node
    .partition;

describe("TestCluster on Kubernetes membership", () => {
  it("joins a silo the Kubernetes way, and every silo converges on one view", async () => {
    const cluster = await start({ initialSilos: 1 });
    const [a] = cluster.silos as [TestSiloHandle];
    const b = await cluster.startAdditionalSilo();

    expect(names(cluster, a)).toEqual(["test-silo-0", "test-silo-1"]);
    expect(names(cluster, b)).toEqual(["test-silo-0", "test-silo-1"]);
    expect(cluster.viewOf(a).appliedVersion).toBe(cluster.viewOf(b).appliedVersion);
    // One view, its own entries included: neither silo has a say over its own status.
    expect(cluster.viewOf(a).membership).toEqual(cluster.viewOf(b).membership);
    const results = await Promise.all(
      Array.from({ length: 40 }, (_, i) =>
        (i % 2 === 0 ? a : b).host.getGrain(Counter, `k${i}`).inc(),
      ),
    );
    expect(results.filter((r) => r !== 1)).toEqual([]);
  });

  it("shows a joiner the same view as its peers: itself joining, and outside the ring", async () => {
    const cluster = await start({ initialSilos: 2 });
    const joiner = await cluster.startAdditionalSilo({ holdReadiness: true });
    await cluster.kubernetes.settle();

    const own = cluster
      .viewOf(joiner)
      .membership.silos.find((m) => m.address.equals(joiner.address));
    expect(own?.status).toBe("joining");
    expect(ringNames(cluster, joiner)).toEqual(["test-silo-0", "test-silo-1"]);
    for (const peer of cluster.silos) {
      expect(cluster.viewOf(peer).membership).toEqual(cluster.viewOf(joiner).membership);
    }
    // Listed, so ready; the kubelet is held, so its pod is not.
    expect(joiner.host.health.ready().ok).toBe(true);

    await cluster.kubernetes.kubelet(joiner);
    expect(ringNames(cluster, joiner)).toEqual(["test-silo-0", "test-silo-1", "test-silo-2"]);
  });

  it("starts a cold cluster: each startup grain activates once, and the ring ends as the ready set", async () => {
    const fromStartup: Record<number, number | string> = {};
    const cluster = await start({
      initialSilos: 0,
      callTimeout: { seconds: 5 },
      configureSilo: (builder, { index }) => {
        builder.addStartupTask(async (grains) => {
          fromStartup[index] = await grains
            .getGrain(Counter, `cold-${index}`)
            .inc()
            .catch((e: unknown) => `error: ${String(e)}`);
        });
      },
    });
    // Three pods listed together, none ready, started at once.
    const silos = await cluster.startAdditionalSilos(3);
    expect(fromStartup).toEqual({ 0: 1, 1: 1, 2: 1 });
    await cluster.kubernetes.settle();

    const keys = ["cold-0", "cold-1", "cold-2"];
    const id = (key: string): GrainId => new GrainId("K8sHarnessCounter", key);
    for (const silo of silos) {
      expect(ringNames(cluster, silo)).toEqual(["test-silo-0", "test-silo-1", "test-silo-2"]);
    }
    for (const key of keys) {
      expect(
        silos.filter((s) => s.host.isActive(id(key))),
        key,
      ).toHaveLength(1);
      expect(
        silos.filter((s) => partitionOf(s).entryFor(id(key)) !== undefined),
        key,
      ).toHaveLength(1);
    }
    // The one activation answers from every silo.
    const again = await Promise.all(
      keys.map((key, i) => silos[i]!.host.getGrain(Counter, key).inc()),
    );
    expect(again).toEqual([2, 2, 2]);
  });

  it("clears a crashed silo out once its pod goes not-ready and then away", async () => {
    const cluster = await start({
      initialSilos: 3,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const [a, b, d] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    for (let i = 0; i < 10; i++) expect(await d.host.getGrain(Counter, `on-d-${i}`).inc()).toBe(1);
    for (let i = 0; i < 10; i++) expect(await a.host.getGrain(Counter, `on-d-${i}`).inc()).toBe(2);

    await cluster.killSilo(d);
    // A crashed pod stays listed until Kubernetes notices.
    expect(names(cluster, a)).toContain("test-silo-2");

    await cluster.kubernetes.markNotReady(d);
    expect(names(cluster, a)).not.toContain("test-silo-2");
    await cluster.kubernetes.removePod(d);
    expect(cluster.kubernetes.api.pods().map((p) => p.name)).not.toContain("test-silo-2");

    // Its grains come back fresh on the survivors.
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        (i % 2 === 0 ? a : b).host.getGrain(Counter, `on-d-${i}`).inc(),
      ),
    );
    expect(results).toEqual(Array.from({ length: 10 }, () => 1));
  });

  it("holds two different views at once while one silo's watch lags, then converges", async () => {
    const cluster = await start({ initialSilos: 3 });
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];

    cluster.kubernetes.hold(a);
    await cluster.kubernetes.markNotReady(c);

    expect(names(cluster, a)).toEqual(["test-silo-0", "test-silo-1", "test-silo-2"]);
    expect(names(cluster, b)).toEqual(["test-silo-0", "test-silo-1"]);
    expect(cluster.viewOf(a).appliedVersion).toBeLessThan(cluster.viewOf(b).appliedVersion);
    expect(cluster.viewOf(a).appliedVersion).toBe(cluster.viewOf(a).membership.version);

    await cluster.kubernetes.release(a);
    expect(names(cluster, a)).toEqual(["test-silo-0", "test-silo-1"]);
    expect(cluster.viewOf(a).appliedVersion).toBe(cluster.viewOf(b).appliedVersion);
  });

  it("catches a silo up through refresh when a peer's call shows it is behind, with no further slice write", async () => {
    const cluster = await start({ initialSilos: 3 });
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    cluster.kubernetes.hold(a);
    await cluster.kubernetes.markNotReady(c);
    expect(cluster.viewOf(a).membership.version).toBeLessThan(cluster.viewOf(b).membership.version);

    // B's directory operations reach A carrying B's version; A refreshes to it.
    const results = await Promise.all(
      Array.from({ length: 40 }, (_, i) =>
        b.host
          .getGrain(Counter, `behind-${i}`)
          .inc()
          .catch((e: unknown) => `error: ${String(e)}`),
      ),
    );
    expect(results.filter((r) => r !== 1)).toEqual([]);
    expect(cluster.viewOf(a).membership.version).toBe(cluster.viewOf(b).membership.version);
    expect(cluster.viewOf(a).membership.silos).toEqual(cluster.viewOf(b).membership.silos);

    // The events held all along are older than what A now holds, and change nothing.
    const caughtUp = cluster.viewOf(a).membership;
    await cluster.kubernetes.release(a);
    expect(cluster.viewOf(a).membership).toEqual(caughtUp);
  });

  it("catches a lagging silo up on a forced relist, held events and all", async () => {
    const cluster = await start({ initialSilos: 3 });
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    cluster.kubernetes.hold(a);
    await cluster.kubernetes.markNotReady(c);
    expect(names(cluster, a)).toHaveLength(3);

    await cluster.kubernetes.relist(a);
    expect(names(cluster, a)).toEqual(["test-silo-0", "test-silo-1"]);
    expect(cluster.viewOf(a).appliedVersion).toBe(cluster.viewOf(b).appliedVersion);
  });

  it("flips a pod's readiness out of and back into the view", async () => {
    const cluster = await start({ initialSilos: 2 });
    const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    await cluster.kubernetes.setReady(b, false);
    expect(names(cluster, a)).toEqual(["test-silo-0"]);
    await cluster.kubernetes.setReady(b, true);
    expect(names(cluster, a)).toEqual(["test-silo-0", "test-silo-1"]);
  });

  it("advertises silo metadata through pod labels", async () => {
    const cluster = await start({
      initialSilos: 2,
      siloMetadata: ({ index }) => ({ role: index === 0 ? "front" : "worker" }),
    });
    const [a] = cluster.silos as [TestSiloHandle];
    const roles = cluster
      .viewOf(a)
      .membership.silos.map((m) => [m.address.podName, m.metadata?.role])
      .sort();
    expect(roles).toEqual([
      ["test-silo-0", "front"],
      ["test-silo-1", "worker"],
    ]);
  });

  it("drains a gracefully stopped silo out of the view", async () => {
    const cluster = await start({ initialSilos: 2 });
    const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    await cluster.stopSilo(b);
    expect(names(cluster, a)).toEqual(["test-silo-0"]);
    expect(cluster.kubernetes.api.pods().map((p) => p.name)).toEqual(["test-silo-0"]);
  });
});

describe("TestCluster on the default membership", () => {
  it("has no Kubernetes controls", async () => {
    const cluster = await TestCluster.start({ initialSilos: 1, reminders: false });
    clusters.push(cluster);
    expect(() => cluster.kubernetes).toThrow(/membership: "kubernetes"/);
    expect(cluster.viewOf(cluster.primary).appliedVersion).toBe(
      cluster.viewOf(cluster.primary).membership.version,
    );
  });
});
