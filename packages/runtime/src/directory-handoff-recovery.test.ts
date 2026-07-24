import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { RejectionError } from "@tsva/core/errors";
import { ConsistentHashRing } from "@tsva/directory/consistent-hash-ring";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@tsva/messaging/transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";

interface ICounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.handoffRecovery");

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

/** First `Counter/*` key the ring assigns to `owner` — arranges a deterministic move on join. */
function counterKeyOwnedBy(ring: ConsistentHashRing, owner: SiloAddress): string {
  for (let i = 0; ; i++) {
    const key = `k-${i}`;
    if (ring.ownerOf(new GrainId("Counter", key)).equals(owner)) return key;
  }
}

/**
 * Wraps an `InProcessTransport` so `connect()` to a chosen target fails the
 * first `failures` times it's attempted, then behaves normally — simulating a
 * transient network fault for `beginRecovery`'s retry-with-backoff.
 */
class FlakyTransport implements Transport {
  private remainingFailures: number;
  constructor(
    private readonly inner: Transport,
    private readonly failTarget: SiloAddress,
    failures: number,
  ) {
    this.remainingFailures = failures;
  }
  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    return this.inner.listen(address, onMessage, onAccept);
  }
  async connect(to: SiloAddress, preamble: ConnectionPreamble): Promise<Connection> {
    if (to.equals(this.failTarget) && this.remainingFailures > 0) {
      this.remainingFailures--;
      throw new RejectionError("simulated transient network failure", "unknownTarget");
    }
    return this.inner.connect(to, preamble);
  }
}

describe("directory handoff recovery (ACK-delete, retry, expiry)", () => {
  beforeEach(() => undefined);

  it("ACKs a successful recovery pull, and the source deletes exactly the served entries", async () => {
    // random -> 0.99 picks the last candidate, forcing the join-handoff path.
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

    const ring3 = new ConsistentHashRing([silo(0), silo(1), silo(2)]);
    const key = counterKeyOwnedBy(ring3, silo(2));

    try {
      await nodes[0]!.getGrain(ICounter, key).increment(5);

      membership.addSilo(silo(2));
      const node2 = makeNode(silo(2));
      await node2.start();
      nodes[0]!.updateView();
      nodes[1]!.updateView();

      // Recovery pulls and ACKs before the call resolves (applyDirectoryOp's
      // register/lookup await it), so by the time this returns silo-1 has
      // already deleted the served entry from its retained handoff snapshot.
      expect(await node2.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(nodes[1]!.pendingHandoffCount()).toBe(0);

      await node2.stop();
    } finally {
      await nodes[0]!.stop();
      await nodes[1]!.stop();
    }
  });

  it("retries a failed recovery pull with backoff and still recovers once the source becomes reachable", async () => {
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    const time = new FakeTimeProvider();
    const flaky = new FlakyTransport(new InProcessTransport(network, CLUSTER), silo(1), 2);

    const node0 = new ClusterNode({
      local: silo(0),
      clusterId: CLUSTER,
      membership: new MembershipView(membership, silo(0)),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0.99,
    });
    node0.registerGrain(CounterGrain, { interfaces: [ICounter] });

    const node1 = new ClusterNode({
      local: silo(1),
      clusterId: CLUSTER,
      membership: new MembershipView(membership, silo(1)),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0.99,
    });
    node1.registerGrain(CounterGrain, { interfaces: [ICounter] });

    await node0.start();
    await node1.start();

    const ring3 = new ConsistentHashRing([silo(0), silo(1), silo(2)]);
    const key = counterKeyOwnedBy(ring3, silo(2));
    const grainId = new GrainId("Counter", key);

    try {
      await node0.getGrain(ICounter, key).increment(5);

      membership.addSilo(silo(2));
      // silo-2's connections go through the flaky transport; its first two
      // pull attempts against silo-1 fail before the third succeeds.
      const node2 = new ClusterNode({
        local: silo(2),
        clusterId: CLUSTER,
        membership: new MembershipView(membership, silo(2)),
        transport: flaky,
        time,
        random: () => 0.99,
        recovery: { maxAttempts: 5, backoffMs: 1_000 },
      });
      node2.registerGrain(CounterGrain, { interfaces: [ICounter] });
      const startPromise = node2.start();
      node0.updateView();
      node1.updateView();

      // Pump microtasks and advance the fake clock in lockstep so each
      // retry's backoff elapses without a real timer.
      for (let i = 0; i < 10; i++) {
        await Promise.resolve();
        await Promise.resolve();
        time.advance(1_000);
      }
      await startPromise;

      // The call reaches the pre-existing activation with its state intact,
      // not a fresh reactivation — recovery succeeded despite the two faults.
      // random->0.99 on two silos picks the last candidate (silo-1), so that's
      // where the original activation landed.
      expect(await node2.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(node1.isActive(grainId)).toBe(true);
      expect(node2.isActive(grainId)).toBe(false);

      await node2.stop();
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });

  it("expires a retained handoff entry that its successor never pulls, past the retention window", async () => {
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    const time = new FakeTimeProvider();

    const makeNode = (local: SiloAddress) =>
      new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        time,
        random: () => 0.99,
        recovery: { retentionMs: 5_000 },
      });

    const node0 = makeNode(silo(0));
    const node1 = makeNode(silo(1));
    node0.registerGrain(CounterGrain, { interfaces: [ICounter] });
    node1.registerGrain(CounterGrain, { interfaces: [ICounter] });
    await node0.start();
    await node1.start();

    const ring3 = new ConsistentHashRing([silo(0), silo(1), silo(2)]);
    const key = counterKeyOwnedBy(ring3, silo(2));

    try {
      await node0.getGrain(ICounter, key).increment(5);

      // silo-2 "joins" the membership view but never actually starts a node
      // (crashed mid-join) — silo-1 never receives a recovery pull for it.
      membership.addSilo(silo(2));
      node0.updateView();
      node1.updateView();

      expect(node1.pendingHandoffCount()).toBe(1);

      time.advance(4_000);
      // A subsequent (unrelated) view change re-checks retention: still within it.
      node1.updateView();
      expect(node1.pendingHandoffCount()).toBe(1);

      time.advance(2_000); // total 6s > the configured 5s retention
      node1.updateView();
      expect(node1.pendingHandoffCount()).toBe(0);
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});
