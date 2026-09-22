import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.drainReadiness");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
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

/** First `Counter/*` key the ring assigns to `owner` — arranges a deterministic move. */
function counterKeyOwnedBy(ring: ConsistentHashRing, owner: SiloAddress): string {
  for (let i = 0; ; i++) {
    const key = `k-${i}`;
    if (ring.ownerOf(new GrainId("Counter", key)).equals(owner)) return key;
  }
}

/**
 * A silo whose readiness flips is not the same thing as a silo that is gone.
 * Graceful shutdown flips readiness and then waits out a grace period before
 * stopping anything, so for that whole window a draining silo is still live and
 * still serving its activations — the directory must keep pointing at it.
 * Only the endpoint *disappearing* means the grain has to reactivate elsewhere.
 */
describe("directory entries across a readiness flip (draining vs gone)", () => {
  beforeEach(() => undefined);

  it("keeps the entry for a silo that is only draining, so calls reach its live activation", async () => {
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    const makeNode = (local: SiloAddress) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        // random -> 0.99 picks the last candidate, placing the activation on silo-1.
        random: () => 0.99,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    const nodes = addresses.map(makeNode);
    for (const n of nodes) await n.start();

    // The entry is owned by silo-0 (the ring owner); random placement puts the
    // activation it points at on silo-1.
    const key = counterKeyOwnedBy(new ConsistentHashRing(addresses), silo(0));
    const grainId = new GrainId("Counter", key);

    try {
      expect(await nodes[0]!.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(nodes[1]!.isActive(grainId)).toBe(true);
      expect(nodes[0]!.partition.lookup(grainId)?.silo.equals(silo(1))).toBe(true);

      // silo-1's readiness flips (a rolling update's drain, or a failed probe):
      // it leaves the ring, but it is still a member of the view and still
      // serving the activation its entry points at.
      membership.setStatus(silo(1), "draining");
      nodes[0]!.updateView();
      nodes[1]!.updateView();

      expect(nodes[0]!.partition.lookup(grainId)?.silo.equals(silo(1))).toBe(true);

      // The call therefore reaches the original activation with its state intact
      // rather than building a second one with divergent state.
      expect(await nodes[0]!.getGrain(ICounter, key).increment(3)).toBe(8);
      expect(nodes[1]!.isActive(grainId)).toBe(true);
      expect(nodes[0]!.isActive(grainId)).toBe(false);
    } finally {
      await nodes[0]!.stop();
      await nodes[1]!.stop();
    }
  });

  it("drops the entry once the silo's endpoint is gone from the view entirely", async () => {
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    const makeNode = (local: SiloAddress) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        random: () => 0.99,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    const nodes = addresses.map(makeNode);
    for (const n of nodes) await n.start();

    const key = counterKeyOwnedBy(new ConsistentHashRing(addresses), silo(0));
    const grainId = new GrainId("Counter", key);

    try {
      expect(await nodes[0]!.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(nodes[0]!.partition.lookup(grainId)?.silo.equals(silo(1))).toBe(true);

      // The endpoint is removed (the pod is gone), not merely not ready: the
      // grain's host will never come back, so its entry is dead weight and the
      // grain reactivates on the next call.
      membership.setStatus(silo(1), "draining");
      membership.removeSilo(silo(1));
      nodes[0]!.updateView();
      nodes[1]!.updateView();

      expect(nodes[0]!.partition.lookup(grainId)).toBeUndefined();
    } finally {
      await nodes[0]!.stop();
      await nodes[1]!.stop();
    }
  });
});
