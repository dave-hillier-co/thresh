import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { RejectionError } from "@thresh/core/errors";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounter extends GrainKey<string> {
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

/** First `Counter/*` key every `(ring, owner)` pair agrees on — arranges a chain of moves. */
function counterKeyMovingThrough(
  moves: ReadonlyArray<{ ring: ConsistentHashRing; owner: SiloAddress }>,
): string {
  for (let i = 0; ; i++) {
    const grainId = new GrainId("Counter", `k-${i}`);
    if (moves.every(({ ring, owner }) => ring.ownerOf(grainId).equals(owner))) return `k-${i}`;
  }
}

/**
 * Wraps an `InProcessTransport` so inbound directory requests from `holdFrom`
 * are parked instead of delivered, until `release()` hands them to the real
 * handler. This is the seam that makes the "membership changes while a
 * recovery pull is in flight" interleaving deterministic: parking the pull
 * REQUEST at the source keeps the puller's `beginRecovery` continuation
 * pending across as many `updateView()` calls as the test wants to drive,
 * with no timers and no sleeping.
 */
class GatedTransport implements Transport {
  private readonly parked: Array<() => void> = [];
  private open = false;
  constructor(
    private readonly inner: Transport,
    private readonly holdFrom: SiloAddress,
  ) {}
  async listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    const gated: MessageHandler = (message, from) => {
      const isHeldPull =
        !this.open &&
        message.direction === "request" &&
        message.system === "directory" &&
        message.targetGrain.type === "$directory" &&
        from.equals(this.holdFrom);
      if (!isHeldPull) return onMessage(message, from);
      this.parked.push(() => void onMessage(message, from));
      return undefined;
    };
    return this.inner.listen(address, gated, onAccept);
  }
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    return this.inner.connect(to, preamble, onMessage);
  }
  /** Deliver every parked request and stop parking. */
  release(): void {
    this.open = true;
    for (const deliver of this.parked.splice(0)) deliver();
  }
}

/** Drain the microtask queue (and one macrotask turn) so in-process delivery settles. */
async function settle(): Promise<void> {
  for (let i = 0; i < 20; i++) await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
  for (let i = 0; i < 20; i++) await Promise.resolve();
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

  it("does not adopt (or ACK-delete) entries whose range moved again while the pull was in flight", async () => {
    // Interleaving under test: silo-2 joins and pulls the range it owns under
    // the 3-silo ring; while that pull is parked at the source, silo-3 joins
    // and the range moves on to silo-3. Orleans orders the newer view's
    // eviction after the older acquire (`WaitForRange`) so the entry is
    // re-snapshotted for the newer owner; this port has no such lock, so the
    // pull's adopt decision must be taken against the ring in force at
    // registration time, and only what was adopted may be ACK-deleted.
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    // Park silo-2's directory pulls at silo-1, the source that retains the entry.
    const gate = new GatedTransport(new InProcessTransport(network, CLUSTER), silo(2));
    const makeNode = (
      local: SiloAddress,
      transport: Transport = new InProcessTransport(network, CLUSTER),
    ) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport,
        random: () => 0.99,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };

    const node0 = makeNode(silo(0));
    const node1 = makeNode(silo(1), gate);
    await node0.start();
    await node1.start();

    // A key silo-1 owns at two silos, silo-2 owns at three, silo-3 at four:
    // silo-1 is the handoff source, silo-2 the stale puller, silo-3 the true owner.
    const key = counterKeyMovingThrough([
      { ring: new ConsistentHashRing([silo(0), silo(1)]), owner: silo(1) },
      { ring: new ConsistentHashRing([silo(0), silo(1), silo(2)]), owner: silo(2) },
      { ring: new ConsistentHashRing([silo(0), silo(1), silo(2), silo(3)]), owner: silo(3) },
    ]);
    const grainId = new GrainId("Counter", key);
    let node2: ClusterNode | undefined;
    let node3: ClusterNode | undefined;

    try {
      expect(await node0.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(node1.pendingHandoffCount()).toBe(0);

      // silo-2 joins: silo-1 hands the range off, silo-2 pulls — and the pull
      // sits parked at silo-1 for the rest of the interleaving.
      membership.addSilo(silo(2));
      node2 = makeNode(silo(2));
      await node2.start();
      node0.updateView();
      node1.updateView();
      await settle();
      expect(node1.pendingHandoffCount()).toBe(1);

      // silo-3 joins while silo-2's pull is still in flight: the range is now
      // silo-3's, and every silo (silo-2 included) has already applied the view.
      membership.addSilo(silo(3));
      node0.updateView();
      node1.updateView();
      node2.updateView();
      await settle();

      gate.release();
      await settle();

      // The served entry belongs to silo-3 now, so silo-2 must neither register
      // it into its own partition (an orphan the current ring says is not its
      // range) nor ACK it away at the source (which would strand silo-3).
      expect(node2.partition.lookup(grainId)).toBeUndefined();
      expect(node1.pendingHandoffCount()).toBe(1);

      // The true owner's own pull therefore still finds the entry: the call
      // reaches the original activation with its state intact (5 + 2), rather
      // than lazily rebuilding a fresh grain (which would answer 2).
      node3 = makeNode(silo(3));
      await node3.start();
      await settle();
      expect(await node3.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(node1.isActive(grainId)).toBe(true);
    } finally {
      await node3?.stop();
      await node2?.stop();
      await node1.stop();
      await node0.stop();
    }
  });
});
