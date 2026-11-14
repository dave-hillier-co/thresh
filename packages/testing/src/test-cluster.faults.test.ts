import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { GrainCallTimeoutError, RejectionError } from "@thresh/core/errors";
import { NetworkFaults } from "@thresh/messaging/fault-injecting-transport";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import {
  TestCluster,
  type TestClusterOptions,
  type TestSiloHandle,
} from "@thresh/testing/test-cluster";

/**
 * Network fault injection through the harness: every silo's transport sits behind a
 * `FaultInjectingTransport` sharing `cluster.faults`. Grains are placed on the silo that first
 * calls them, so a call from another silo crosses exactly one link.
 */
const Echo = defineGrain("FaultsEcho", () => ({
  echo: async (value: string): Promise<string> => value,
}));

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(options: Partial<TestClusterOptions> = {}): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    ...options,
    initialSilos: 3,
    grains: [Echo],
    defaultPlacementStrategy: new PreferLocalPlacement(),
    callTimeout: { ms: 300 },
    reminders: false,
  });
  clusters.push(cluster);
  return cluster;
}

/** Activate `key` on `home`, so later calls from other silos go to it over the wire. */
async function placeOn(home: TestSiloHandle, key: string): Promise<void> {
  expect(await home.host.getGrain(Echo, key).echo("placed")).toBe("placed");
}

describe("TestCluster network faults", () => {
  it("times a dropped request out at the caller", async () => {
    const cluster = await start();
    const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    await placeOn(b, "on-b");
    expect(await a.host.getGrain(Echo, "on-b").echo("warm")).toBe("warm");

    cluster.faults.drop({ from: a.address, to: b.address });
    const error = await a.host
      .getGrain(Echo, "on-b")
      .echo("lost")
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(GrainCallTimeoutError);
    expect(cluster.faults.dropped).toBeGreaterThan(0);
  });

  it("fails calls across a partition, leaves other links alone, and heals", async () => {
    const cluster = await start();
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    await placeOn(b, "on-b");
    await placeOn(c, "on-c");
    // Fill the callers' location caches first: an uncached call resolves through the grain
    // directory, whose owner for a key may sit on the far side of the partition.
    expect(await a.host.getGrain(Echo, "on-c").echo("warm")).toBe("warm");
    expect(await c.host.getGrain(Echo, "on-b").echo("warm")).toBe("warm");

    const partition = cluster.faults.partition([a.address], [b.address]);
    await expect(a.host.getGrain(Echo, "on-b").echo("across")).rejects.toBeDefined();
    await expect(a.host.getGrain(Echo, "on-c").echo("a to c")).resolves.toBe("a to c");
    await expect(c.host.getGrain(Echo, "on-b").echo("c to b")).resolves.toBe("c to b");

    partition.heal();
    await expect(a.host.getGrain(Echo, "on-b").echo("healed")).resolves.toBe("healed");
  });

  it("rejects a call whose connect fails with the transport's rejection", async () => {
    // Set before the cluster starts, so no connection from silo 0 to silo 1 is ever pooled.
    const faults = new NetworkFaults();
    faults.failConnects({ from: "test-silo-0", to: "test-silo-1" });
    const cluster = await start({ faults });
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    await placeOn(b, "on-b");

    const error = await a.host
      .getGrain(Echo, "on-b")
      .echo("x")
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RejectionError);
    expect((error as RejectionError).kind).toBe("unknownTarget");
    expect(faults.refusedConnects).toBeGreaterThan(0);
    await expect(c.host.getGrain(Echo, "on-b").echo("c to b")).resolves.toBe("c to b");
  });
});
