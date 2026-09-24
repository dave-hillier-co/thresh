import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.forwardCacheInvalidation");

// Module-level, not instance state: `migrateRandomActivations` below moves the
// activation WITHOUT a migration participant opted in, so it reactivates
// fresh on the target (Orleans semantics for a grain that declares no
// `IGrainMigrationParticipant`) -- this test is about routing, not state
// transfer, so the call count lives outside the grain instance instead.
let callCount = 0;

@grain()
class CounterGrain extends Grain implements ICounter {
  async increment(_by: number): Promise<number> {
    callCount += 1;
    return callCount;
  }
}

const CLUSTER = "c1";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}
  current() {
    return this.shared.current();
  }
  updates() {
    return this.shared.updates();
  }
  localSilo() {
    return this.local;
  }
}

// Issue #110: a silo that no longer hosts a grain forwarded a call for it to
// the real owner with no signal telling the ORIGINAL caller its cached
// address was stale, so the caller's `LocationCache` kept pointing at the
// wrong silo forever -- every later call paid an extra forwarding hop
// indefinitely (Orleans instead sends a cache-invalidation header on forward,
// `MessageCenter.AddToCacheInvalidationHeader`).
describe("forwarding invalidates the caller's stale cache entry (issue #110)", () => {
  it("evicts the caller's cached address once the call it fronted has to be forwarded on", async () => {
    callCount = 0;
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1), silo(2)]);
    const makeNode = (local: SiloAddress, random: () => number) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        random,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    // Candidates for a fresh placement are `[silo-0, silo-1, silo-2]` (membership
    // order); silo-A's own random (0.4) resolves to index 1 -> silo-1.
    const nodeA = makeNode(silo(0), () => 0.4);
    const nodeB = makeNode(silo(1), () => 0);
    const nodeC = makeNode(silo(2), () => 0);
    await nodeA.start();
    await nodeB.start();
    await nodeC.start();

    try {
      const grainRef = nodeA.getGrain(ICounter, "g1");

      // First call: cache AND directory miss (nothing exists yet), so it goes
      // through placement, landing on silo-1. A placement decision alone
      // never populates the caller's cache (only a directory HIT does, below).
      expect(await grainRef.increment(1)).toBe(1);
      expect(nodeA.directoryCacheStats()).toMatchObject({ misses: 1, hits: 0 });

      // Second call, still before the migration: cache miss, but the
      // directory now has silo-1's entry from the call above, so this HITS
      // the directory and, per `deliver`, caches the resolved address.
      expect(await grainRef.increment(1)).toBe(2);
      expect(nodeA.directoryCacheStats()).toMatchObject({ misses: 2, hits: 0 });

      // A real migration: the activation and its directory entry move from
      // silo-1 to silo-2. A's cache still says silo-1.
      const moved = await nodeB.migrateRandomActivations(silo(2), 1);
      expect(moved).toBe(1);

      // Third call: A's now-stale cache HITS silo-1, which finds no local
      // activation, loses its own directory CAS to silo-2, and forwards there
      // -- transparently relaying the reply back to A.
      expect(await grainRef.increment(1)).toBe(3);
      expect(nodeA.directoryCacheStats()).toMatchObject({ misses: 2, hits: 1 });

      // Fourth call: the forward above must have told A to evict its stale
      // silo-1 entry, so this one is a cache MISS (re-resolved via the
      // directory straight to silo-2) rather than another hit on silo-1 --
      // without this, every future call would keep paying a silo-1 round trip
      // it can never satisfy, bounded only by the call timeout (issue #110).
      const before = nodeA.directoryCacheStats();
      expect(await grainRef.increment(1)).toBe(4);
      const after = nodeA.directoryCacheStats();
      expect(after.misses).toBe(before.misses + 1);
      expect(after.hits).toBe(before.hits);
    } finally {
      await nodeA.stop();
      await nodeB.stop();
      await nodeC.stop();
    }
  });
});
