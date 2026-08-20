import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.cluster");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

// preferLocal makes each node try to activate locally, forcing a CAS race.
interface ILocal extends GrainKey<string> {
  ping(): Promise<string>;
}
const ILocal = defineGrainInterface<ILocal>("ILocal.cluster");

@grain({ placement: "preferLocal" })
class LocalGrain extends Grain implements ILocal {
  async ping(): Promise<string> {
    return "pong";
  }
}

function buildCluster(count: number, opts: { random?: () => number } = {}) {
  // Default deterministic placement -> first candidate (silo-0).
  return TestCluster.start({
    clusterId: "c1",
    initialSilos: count,
    random: opts.random ?? (() => 0),
    grains: [
      { ctor: CounterGrain, interfaces: [ICounter] },
      { ctor: LocalGrain, interfaces: [ILocal] },
    ],
  });
}

const hostsOf = (cluster: TestCluster, grainId: GrainId): ReadonlyArray<TestSiloHandle> =>
  cluster.silos.filter((s) => s.host.isActive(grainId));

/**
 * The address a not-yet-started additional silo will get: mirrors
 * `TestCluster`'s internal `test-silo-${index}` naming so a key's post-join
 * directory owner can be computed before the silo actually joins.
 */
const predictedAddress = (index: number) =>
  new SiloAddress(`test-silo-${index}`, `uid-${index}`, `test-silo-${index}:11111`);

/** First `Counter/*` key the ring assigns to `owner` — to arrange a deterministic move. */
function counterKeyOwnedBy(ring: ConsistentHashRing, owner: SiloAddress): string {
  for (let i = 0; ; i++) {
    const key = `k-${i}`;
    if (ring.ownerOf(new GrainId("Counter", key)).equals(owner)) return key;
  }
}

describe("multi-silo cluster", () => {
  beforeEach(() => undefined);

  it("routes a call from one silo to a grain placed on another, sharing its state", async () => {
    const cluster = await buildCluster(3);
    try {
      // random->0 places "Counter/shared" on silo-0; calls from other silos route there.
      const r1 = await cluster.silos[1]!.host.getGrain(ICounter, "shared").increment(5);
      const r2 = await cluster.silos[2]!.host.getGrain(ICounter, "shared").increment(3);
      expect(r1).toBe(5);
      expect(r2).toBe(8); // same activation, state shared across the cluster

      const hosts = hostsOf(cluster, new GrainId("Counter", "shared"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.silos[0]); // placed on silo-0, not the callers
    } finally {
      await cluster.dispose();
    }
  });

  it("activates a grain on exactly one silo regardless of which silos call it", async () => {
    const cluster = await buildCluster(3);
    try {
      await Promise.all(cluster.silos.map((s) => s.host.getGrain(ICounter, "one").increment(1)));
      expect(hostsOf(cluster, new GrainId("Counter", "one"))).toHaveLength(1);
    } finally {
      await cluster.dispose();
    }
  });

  it("resolves a forced activation race to a single winner via directory CAS", async () => {
    const cluster = await buildCluster(3);
    try {
      const results = await Promise.all(
        cluster.silos.map((s) => s.host.getGrain(ILocal, "race").ping()),
      );
      expect(results).toEqual(["pong", "pong", "pong"]);
      expect(hostsOf(cluster, new GrainId("Local", "race"))).toHaveLength(1);
    } finally {
      await cluster.dispose();
    }
  });

  it("rebalances when a silo leaves: the grain reactivates elsewhere on next call", async () => {
    const cluster = await buildCluster(2);
    const grainId = new GrainId("Counter", "moves");
    try {
      // random->0 places it on silo-0; build up some state.
      expect(await cluster.silos[1]!.host.getGrain(ICounter, "moves").increment(5)).toBe(5);
      expect(hostsOf(cluster, grainId)).toEqual([cluster.silos[0]]);

      // silo-0 leaves the cluster (graceful: mirrors the host loop's drain-then-depart).
      const survivor = cluster.silos[1]!;
      await cluster.stopSilo(cluster.silos[0]!);

      // Next call reactivates the grain on the surviving silo, with fresh state.
      expect(await survivor.host.getGrain(ICounter, "moves").increment(7)).toBe(7);
      expect(hostsOf(cluster, grainId)).toEqual([survivor]);
    } finally {
      await cluster.dispose();
    }
  });

  it("hands off a moved range on join so the grain is not reactivated", async () => {
    // random -> 0.99 picks the LAST candidate: silo-1 of two, silo-2 of three.
    // So on a directory miss the grain would reactivate on the newcomer (silo-2),
    // not its existing host (silo-1) — making a lost handoff observable.
    const cluster = await buildCluster(2, { random: () => 0.99 });
    try {
      // A key the post-join ring assigns to the newcomer, so silo-2 becomes its
      // directory owner and must recover the entry to keep the grain in place.
      const ring3 = new ConsistentHashRing([
        cluster.silos[0]!.address,
        cluster.silos[1]!.address,
        predictedAddress(2),
      ]);
      const key = counterKeyOwnedBy(ring3, predictedAddress(2));
      const grainId = new GrainId("Counter", key);

      // Activate on silo-1 (the last of two candidates) and build up state.
      expect(await cluster.silos[0]!.host.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(hostsOf(cluster, grainId)).toEqual([cluster.silos[1]]);

      // silo-2 joins; it owns the key's range now and recovers it on start. The
      // incumbents adopt the new view automatically, the way the host loop would.
      const node2 = await cluster.startAdditionalSilo();
      expect(node2.address.equals(predictedAddress(2))).toBe(true);

      // Calling from the new owner immediately after the join: the lookup waits
      // for recovery rather than missing, so the call reaches the original host
      // with its state intact (7), and no second activation is created.
      expect(await node2.host.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(hostsOf(cluster, grainId)).toEqual([cluster.silos[1]]);
    } finally {
      await cluster.dispose();
    }
  });
});
