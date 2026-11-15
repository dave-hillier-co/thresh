import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainDirectoryPartition } from "@thresh/directory/grain-directory-partition";
import { mayResend } from "@thresh/messaging/resend-policy";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import {
  TestCluster,
  type TestClusterOptions,
  type TestSiloHandle,
} from "@thresh/testing/test-cluster";

/**
 * A dead process that is still listed (docs/design-membership-and-directory.md): a pod whose
 * process has died stays listed, not ready, until Kubernetes removes it or restarts its container
 * in place. The grains it hosted answer again, from a fresh activation, while it is listed.
 */
const Counter = defineGrain("DeadHostCounter", () => {
  let n = 0;
  return { inc: async (): Promise<number> => ++n };
});
const id = (key: string): GrainId => new GrainId("DeadHostCounter", key);

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(options: Partial<TestClusterOptions> = {}): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    membership: "kubernetes",
    grains: [Counter],
    reminders: false,
    callTimeout: { seconds: 3 },
    ...options,
  });
  clusters.push(cluster);
  return cluster;
}

const partitionOf = (silo: TestSiloHandle): GrainDirectoryPartition =>
  (silo.host as unknown as { parts: { node: { partition: GrainDirectoryPartition } } }).parts.node
    .partition;

/** The entry `key`'s owner holds, read straight from its partition. */
async function entryOf(cluster: TestCluster, key: string): Promise<GrainAddress | undefined> {
  const hash = id(key).getUniformHashCode();
  const owner = cluster.silos.find((s) => partitionOf(s).view.owns(s.address, hash));
  if (owner === undefined) throw new Error(`no silo owns ${key}`);
  const partition = partitionOf(owner);
  const result = await partition.lookup(partition.view.version, id(key));
  if (result.kind !== "ok") throw new Error(`${owner.address.podName} does not own ${key}`);
  return result.value;
}

const keysFor = (prefix: string, n: number): string[] =>
  Array.from({ length: n }, (_, i) => `${prefix}-${i}`);

/** What each call answered, and every call that failed, with the phase it failed in. */
class Results {
  readonly values = new Map<string, number[]>();
  readonly errors: Array<{ phase: string; error: unknown }> = [];
  phase = "start";

  async call(silo: TestSiloHandle, key: string): Promise<number | undefined> {
    const phase = this.phase;
    try {
      const value = await silo.host.getGrain(Counter, key).inc();
      this.values.set(key, [...(this.values.get(key) ?? []), value]);
      return value;
    } catch (error) {
      this.errors.push({ phase, error });
      return undefined;
    }
  }

  /** How many answers `key` had had: a mark to read later answers from. */
  mark(): Map<string, number> {
    return new Map([...this.values].map(([key, list]) => [key, list.length]));
  }

  /**
   * Keys whose answers since `mark` are not one counter counting on: `1, 2, ...` for a key in
   * `fresh` (a new activation), and on from its last answer before the mark for any other.
   */
  notCountingOn(mark: Map<string, number>, fresh: readonly string[]): string[] {
    const out: string[] = [];
    for (const [key, list] of this.values) {
      const seen = mark.get(key) ?? 0;
      const since = list.slice(seen).sort((a, b) => a - b);
      const from = fresh.includes(key) ? 0 : (list[seen - 1] ?? 0);
      if (since.some((value, i) => value !== from + i + 1)) out.push(`${key}: ${list.join(",")}`);
    }
    return out;
  }

  failedIn(phase: string): string[] {
    return this.errors.filter((e) => e.phase === phase).map((e) => String(e.error));
  }
}

function activeTwice(cluster: TestCluster, keys: Iterable<string>): string[] {
  const out: string[] = [];
  for (const key of keys) {
    if (cluster.silos.filter((s) => s.host.isActive(id(key))).length > 1) out.push(key);
  }
  return out;
}

/** Every key, called once through the silos in turn, all at once. */
async function callAll(
  results: Results,
  silos: readonly TestSiloHandle[],
  keys: readonly string[],
  offset = 0,
): Promise<Map<string, number | undefined>> {
  const answers = await Promise.all(
    keys.map((key, i) => results.call(silos[(i + offset) % silos.length]!, key)),
  );
  return new Map(keys.map((key, i) => [key, answers[i]]));
}

describe("a dead process that is still listed", () => {
  it("answers every grain the dead silo hosted from a fresh activation while its pod is listed not ready", async () => {
    const cluster = await start({ initialSilos: 4 });
    const results = new Results();
    const keys = keysFor("crash", 90);
    await callAll(results, cluster.silos, keys);
    await cluster.checkIntegrity();

    const victim = cluster.silos[1]!;
    const survivors = cluster.silos.filter((s) => s !== victim);
    const onVictim = keys.filter((key) => victim.host.isActive(id(key)));
    expect(onVictim.length).toBeGreaterThan(0);

    // Between the kill and the not-ready view the dead silo is still in the ring: calls that need
    // it may be refused, as calls the caller may resend, and nothing else fails.
    results.phase = "killed";
    const mark = results.mark();
    await cluster.killSilo(victim);
    const notReady = cluster.kubernetes.markNotReady(victim);
    const during = callAll(results, survivors, keys, 1);
    await notReady;
    await during;
    for (const { error } of results.errors) {
      expect(error).toBeInstanceOf(RejectionError);
      expect(mayResend(error), String(error)).toBe(true);
    }
    await cluster.checkIntegrity();

    // Once the not-ready view is applied, every grain answers through every survivor.
    results.phase = "not-ready";
    for (let round = 0; round < survivors.length; round++) {
      await callAll(results, survivors, keys, round);
      await cluster.checkIntegrity();
    }
    expect(results.failedIn("not-ready")).toEqual([]);
    expect(results.notCountingOn(mark, onVictim)).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
    expect(cluster.kubernetes.listed(victim)).toBe(true);
  }, 60_000);

  it("removes nothing when the dial to a host times out: the grain is not activated twice", async () => {
    const cluster = await start({
      initialSilos: 3,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const [caller, , host] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const keys = keysFor("unreachable", 20);
    for (const key of keys) await host.host.getGrain(Counter, key).inc();
    const key = keys.find(
      (k) => !partitionOf(caller).view.owns(host.address, id(k).getUniformHashCode()),
    )!;
    expect(await caller.host.getGrain(Counter, key).inc()).toBe(2);
    await cluster.checkIntegrity();

    // The host is alive, and the caller cannot reach it: its dials time out.
    const blocked = cluster.faults.failConnects({ from: caller.address, to: host.address });
    await cluster.faults.sever({ from: caller.address, to: host.address });
    const err = await caller.host
      .getGrain(Counter, key)
      .inc()
      .catch((e: unknown) => e);
    expect(String(err)).toContain("unable to connect to");
    // The entry is still there (the integrity check cannot run while the caller cannot reach
    // the host: it asks every member).
    expect((await entryOf(cluster, key))?.silo).toEqual(host.address);
    expect(cluster.silos.filter((s) => s.host.isActive(id(key)))).toEqual([host]);

    // Once it can reach the host again, the same activation answers.
    blocked.heal();
    expect(await caller.host.getGrain(Counter, key).inc()).toBe(3);
    await cluster.checkIntegrity();
  }, 60_000);

  it("answers a hundred concurrent calls to one grain on a dead pod from one fresh activation", async () => {
    const cluster = await start({
      initialSilos: 4,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const victim = cluster.silos[2]!;
    const survivors = cluster.silos.filter((s) => s !== victim);
    // Grains hosted on the victim whose entry a survivor owns, so it outlives the victim's ranges.
    const keys = keysFor("hundred", 40);
    for (const key of keys) await victim.host.getGrain(Counter, key).inc();
    const ring = partitionOf(survivors[0]!).view;
    const key = keys.find((k) => !ring.owns(victim.address, id(k).getUniformHashCode()))!;
    expect(key).toBeDefined();
    // Every survivor holds a route to it.
    for (const silo of survivors)
      expect(await silo.host.getGrain(Counter, key).inc()).toBeGreaterThan(1);
    const results = new Results();

    await cluster.killSilo(victim);
    await cluster.kubernetes.markNotReady(victim);
    results.phase = "hundred";
    const answers = await Promise.all(
      Array.from({ length: 100 }, (_, i) => results.call(survivors[i % survivors.length]!, key)),
    );
    await cluster.checkIntegrity();

    expect(results.failedIn("hundred")).toEqual([]);
    expect([...answers].sort((a, b) => a! - b!)).toEqual(
      Array.from({ length: 100 }, (_, i) => i + 1),
    );
    expect(cluster.silos.filter((s) => s.host.isActive(id(key)))).toHaveLength(1);
    expect(cluster.kubernetes.listed(victim)).toBe(true);
  }, 60_000);
});

describe("a container that restarts in place", () => {
  it("starts a new process at the same silo address, listed throughout, not ready until it passes readiness", async () => {
    const cluster = await start({ initialSilos: 3 });
    const old = cluster.silos[1]!;
    const peer = cluster.silos[0]!;
    const statusOf = (silo: TestSiloHandle): string | undefined =>
      cluster.viewOf(peer).membership.silos.find((s) => s.address.equals(silo.address))?.status;

    const restarted = await cluster.restartInPlace(old, { holdReadiness: true });

    expect(restarted).not.toBe(old);
    expect(restarted.address.equals(old.address)).toBe(true);
    expect(cluster.silos).toContain(restarted);
    expect(cluster.silos).not.toContain(old);
    expect(cluster.kubernetes.listed(restarted)).toBe(true);
    expect(statusOf(restarted)).toBe("joining");
    expect(partitionOf(restarted).view.inRing(restarted.address)).toBe(false);

    await cluster.kubernetes.kubelet(restarted);
    expect(statusOf(restarted)).toBe("active");
    expect(partitionOf(peer).view.inRing(restarted.address)).toBe(true);
    await cluster.checkIntegrity();
  }, 60_000);

  it("restarts a silo already killed whose pod is still listed, and lets the kubelet mark it ready", async () => {
    const cluster = await start({ initialSilos: 2 });
    const old = cluster.silos[1]!;
    await cluster.killSilo(old);

    const restarted = await cluster.restartInPlace(old);

    expect(restarted.address.equals(old.address)).toBe(true);
    expect(partitionOf(cluster.silos[0]!).view.inRing(restarted.address)).toBe(true);
    expect(await restarted.host.getGrain(Counter, "after-restart").inc()).toBe(1);
    await cluster.checkIntegrity();
  }, 60_000);

  it("answers the grains the old process hosted from fresh activations in the ring, then rejoins", async () => {
    const cluster = await start({ initialSilos: 4 });
    const results = new Results();
    const keys = keysFor("restart", 90);
    // Every silo calls every key, so every survivor holds a route to each.
    for (let round = 0; round < 4; round++) await callAll(results, cluster.silos, keys, round);
    await cluster.checkIntegrity();
    const old = cluster.silos[2]!;
    const survivors = cluster.silos.filter((s) => s !== old);
    const onOld = keys.filter((key) => old.host.isActive(id(key)));
    expect(onOld.length).toBeGreaterThan(0);
    const mark = results.mark();

    // Calls in flight while the container dies and restarts may be refused while the dead process
    // is still in the ring, as calls the caller may resend.
    results.phase = "restarting";
    const restarting = cluster.restartInPlace(old, { holdReadiness: true });
    const during = callAll(results, survivors, keys, 1);
    const restarted = await restarting;
    await during;
    for (const { error } of results.errors) {
      expect(error).toBeInstanceOf(RejectionError);
      expect(mayResend(error), String(error)).toBe(true);
    }

    // The new process is listed and not ready. Survivors still hold routes and entries naming the
    // pod: it removes each entry and refuses, and the grain answers from the ring.
    results.phase = "held";
    for (let round = 0; round < survivors.length; round++) {
      await callAll(results, survivors, keys, round);
      await cluster.checkIntegrity();
    }
    expect(results.failedIn("held")).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
    expect(keys.filter((key) => restarted.host.isActive(id(key)))).toEqual([]);
    for (const key of keys) {
      expect((await entryOf(cluster, key))?.silo.equals(restarted.address)).not.toBe(true);
    }

    // Readiness passes: it rejoins the ring, and every grain keeps counting where it was.
    results.phase = "ready";
    const ready = cluster.kubernetes.kubelet(restarted);
    const whileJoining = callAll(results, cluster.silos, keys, 2);
    await ready;
    await whileJoining;
    await callAll(results, cluster.silos, keys, 3);
    await cluster.checkIntegrity();
    expect(results.failedIn("ready")).toEqual([]);
    expect(results.notCountingOn(mark, onOld)).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
  }, 60_000);
});
