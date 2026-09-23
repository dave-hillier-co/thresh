import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Logger } from "@thresh/core/logger";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { RejectionError } from "@thresh/core/errors";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import type { Message } from "@thresh/messaging/message";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";
import { ClusterNode, type ClusterNodeOptions } from "@thresh/runtime/cluster-node";
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
 * Wraps an `InProcessTransport` so inbound messages from `holdFrom` that `holds`
 * accepts are parked instead of delivered, until `release()` hands them to the
 * real handler. This is the seam that makes an interleaving around a recovery
 * pull deterministic, with no timers and no sleeping. Two are driven with it:
 * parking the pull REQUEST at the source (default) keeps the puller's
 * `beginRecovery` continuation pending across as many `updateView()` calls as the
 * test wants to drive; parking the pull's RESPONSE at the puller keeps it from
 * adopting — and from ACKing what it adopted — for just as long.
 */
class GatedTransport implements Transport {
  private readonly parked: Array<() => void> = [];
  private open = false;
  constructor(
    private readonly inner: Transport,
    private readonly holdFrom: SiloAddress,
    private readonly holds: (message: Message) => boolean = (message) =>
      message.direction === "request" &&
      message.system === "directory" &&
      message.targetGrain.type === "$directory",
  ) {}
  async listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    const gated: MessageHandler = (message, from) => {
      const isHeld = !this.open && from.equals(this.holdFrom) && this.holds(message);
      if (!isHeld) return onMessage(message, from);
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

describe("range recovery on a view change (a range that comes back)", () => {
  beforeEach(() => undefined);

  it("adopts back a range it re-acquires from its own retained snapshot instead of stranding the entry", async () => {
    // Interleaving under test: a silo whose range is taken away keeps the entry
    // in its own handoffSnapshot, and the range then comes back to it. Orleans
    // runs AcquireRangeAsync for the added range on every partition on every
    // view change; here recovery ran only on the JOIN transition, so an
    // incumbent that regained a range never looked — not even at the entry it
    // was still holding itself — and the next call built a second activation
    // while the retained copy quietly expired.
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    // silo-0 places locally (first candidate); silo-1's own placement would land
    // on itself (last candidate), which is what makes a missed lookup diverge
    // into a second activation rather than a forward to the original one.
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
    const node0 = makeNode(silo(0), () => 0);
    const node1 = makeNode(silo(1), () => 0.99);
    await node0.start();
    await node1.start();

    // silo-1 owns the entry; silo-2 owns the range once it appears in the view.
    const key = counterKeyMovingThrough([
      { ring: new ConsistentHashRing(addresses), owner: silo(1) },
      { ring: new ConsistentHashRing([...addresses, silo(2)]), owner: silo(2) },
    ]);
    const grainId = new GrainId("Counter", key);

    try {
      expect(await node0.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(node1.partition.lookup(grainId)?.silo.equals(silo(0))).toBe(true);

      // silo-2 joins the view but never starts: silo-1 hands the range off into
      // its snapshot and nobody is there to pull it.
      membership.addSilo(silo(2));
      node0.updateView();
      node1.updateView();
      expect(node1.pendingHandoffCount()).toBe(1);

      // silo-2 leaves again and the range returns to silo-1, which is already
      // active — and still holding the entry in its own snapshot.
      membership.removeSilo(silo(2));
      node0.updateView();
      node1.updateView();

      // The entry is back in the live partition, and no longer retained as a
      // handoff for a successor that will never come.
      expect(node1.partition.lookup(grainId)?.silo.equals(silo(0))).toBe(true);
      expect(node1.pendingHandoffCount()).toBe(0);

      // A call reaches the original activation with its state intact, rather
      // than building a second activation of a grain that never stopped running.
      expect(await node1.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(node0.isActive(grainId)).toBe(true);
      expect(node1.isActive(grainId)).toBe(false);
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });

  it("pulls a range it gains when a peer leaves the ring, from the peer still holding the entries", async () => {
    // The same gap on the other side, and the one a rolling update hits: nobody
    // joins, so no join recovery runs — a peer leaves the ring instead (here by
    // starting to drain, which is how a readiness flip presents), the range it
    // gives up lands on a silo that was already active, and that incumbent never
    // went looking for the entries. The departing peer keeps them for a
    // successor that will never ask, and they expire where they sit.
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1), silo(2)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    // silo-0 places locally (first candidate); silo-1's own placement would land
    // on the last candidate, so a lookup that misses ends in a fresh activation
    // rather than a forward to the original one.
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
    const node0 = makeNode(silo(0), () => 0);
    const node1 = makeNode(silo(1), () => 0.99);
    const node2 = makeNode(silo(2), () => 0);
    await node0.start();
    await node1.start();
    await node2.start();

    // silo-2 owns the entry at three silos; silo-1 inherits its range when
    // silo-2 leaves the ring.
    const key = counterKeyMovingThrough([
      { ring: new ConsistentHashRing(addresses), owner: silo(2) },
      { ring: new ConsistentHashRing([silo(0), silo(1)]), owner: silo(1) },
    ]);
    const grainId = new GrainId("Counter", key);

    try {
      expect(await node0.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(node2.partition.lookup(grainId)?.silo.equals(silo(0))).toBe(true);

      // silo-2 starts to drain: out of the ring, still in the view and still
      // holding the entry — which its successor now has to come and get.
      membership.setStatus(silo(2), "draining");
      node0.updateView();
      node1.updateView();
      node2.updateView();
      await settle();

      expect(node1.partition.lookup(grainId)?.silo.equals(silo(0))).toBe(true);
      expect(node2.pendingHandoffCount()).toBe(0);

      expect(await node1.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(node0.isActive(grainId)).toBe(true);
      expect(node1.isActive(grainId)).toBe(false);
    } finally {
      await node0.stop();
      await node1.stop();
      await node2.stop();
    }
  });
});

describe("recovery gating is per source (a slow peer holds only its own ranges)", () => {
  beforeEach(() => undefined);

  it("serves an op for a range another source owed while a source's pull is still out", async () => {
    // `awaitRecovered` awaited the WHOLE multi-source pull, so one slow-but-present
    // peer blocked every owned directory operation on this silo — each attempt
    // bounded by the call timeout, the whole budget by the retry count, ~90s in
    // all — and remote registers into those ranges failed on the caller's own
    // deadline. A grain's entry can only come from the source that owned its range
    // before the change, so that is the only pull its operation has to wait for.
    const network = new InProcessNetwork();
    const addresses = [silo(0), silo(1), silo(2)];
    const membership = new StaticMembershipService(addresses[0]!, addresses);
    // Park silo-3's directory pulls arriving at silo-1.
    const gate = new GatedTransport(new InProcessTransport(network, CLUSTER), silo(3));
    const makeNode = (
      local: SiloAddress,
      transport: Transport = new InProcessTransport(network, CLUSTER),
    ) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport,
        // random -> 0 places every activation on silo-0 (the first candidate).
        random: () => 0,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    // The gate is silo-1's listener, parking the pulls silo-3 sends it.
    const nodes = [makeNode(silo(0)), makeNode(silo(1), gate), makeNode(silo(2))];
    for (const n of nodes) await n.start();

    const ring4 = new ConsistentHashRing([...addresses, silo(3)]);
    // Two grains whose entries sit with different owners, both moving to silo-3.
    const keyA = counterKeyMovingThrough([
      { ring: new ConsistentHashRing(addresses), owner: silo(1) },
      { ring: ring4, owner: silo(3) },
    ]);
    const keyB = counterKeyMovingThrough([
      { ring: new ConsistentHashRing(addresses), owner: silo(2) },
      { ring: ring4, owner: silo(3) },
    ]);
    const grainA = new GrainId("Counter", keyA);
    let node3: ClusterNode | undefined;

    try {
      expect(await nodes[0]!.getGrain(ICounter, keyA).increment(5)).toBe(5);
      expect(await nodes[0]!.getGrain(ICounter, keyB).increment(5)).toBe(5);
      expect(nodes[1]!.partition.lookup(grainA)?.silo.equals(silo(0))).toBe(true);
      expect(nodes[2]!.partition.lookup(new GrainId("Counter", keyB))?.silo.equals(silo(0))).toBe(
        true,
      );

      // silo-3 joins: both ranges move to it and both owners retain their entry for
      // it. Its pull from silo-2 is answered and adopted; the one from silo-1 stays
      // parked, so that source is still outstanding.
      membership.addSilo(silo(3));
      node3 = makeNode(silo(3));
      const started = node3.start();
      for (const n of nodes) n.updateView();
      await started;
      await settle();
      expect(nodes[1]!.pendingHandoffCount()).toBe(1);
      expect(nodes[2]!.pendingHandoffCount()).toBe(0);

      let completedA = false;
      const callA = node3.getGrain(ICounter, keyA).increment(2);
      void callA.finally(() => (completedA = true));
      let completedB = false;
      const callB = node3.getGrain(ICounter, keyB).increment(1);
      void callB.finally(() => (completedB = true));
      await settle();

      // The op for silo-2's range is served at once, and reaches the original
      // activation with its state intact.
      expect(completedB).toBe(true);
      expect(await callB).toBe(6);

      // The one whose entry silo-1 is still holding waits for that pull — and gets
      // its entry as soon as it lands.
      expect(completedA).toBe(false);
      gate.release();
      await settle();
      expect(await callA).toBe(7);
      expect(nodes[1]!.pendingHandoffCount()).toBe(0);
    } finally {
      await node3?.stop();
      for (const n of nodes) await n.stop();
    }
  });
});

describe("recovery exhaustion (one shot for the process lifetime, silently swallowed)", () => {
  beforeEach(() => undefined);

  it("re-arms on a backoff, and counts and logs the exhaustion, instead of abandoning the range", async () => {
    // A peer merely slow to accept connections at join time used to cost the joiner
    // those ranges for the rest of the process's life — a few attempts and ~400ms
    // of tolerance, one shot, with the failure swallowed by a bare `catch`, so
    // nothing at runtime said the ranges had degraded to lazy reactivation.
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const time = new FakeTimeProvider();
    const warnings: string[] = [];
    const logger: Logger = {
      debug() {},
      info() {},
      warn: (message) => void warnings.push(message),
      error() {},
    };
    // Exactly the in-pass budget's worth of connection failures (two attempts):
    // the join pull exhausts, and only the re-armed one gets through.
    const flaky = new FlakyTransport(new InProcessTransport(network, CLUSTER), silo(1), 2);
    const makeNode = (
      local: SiloAddress,
      transport: Transport,
      extra: Partial<ClusterNodeOptions>,
    ) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport,
        time,
        random: () => 0.99,
        ...extra,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    const node0 = makeNode(silo(0), new InProcessTransport(network, CLUSTER), {});
    const node1 = makeNode(silo(1), new InProcessTransport(network, CLUSTER), {});
    await node0.start();
    await node1.start();

    // The entry sits with silo-1 at two silos and moves to silo-2 on the join.
    const key = counterKeyMovingThrough([
      { ring: new ConsistentHashRing([silo(0), silo(1)]), owner: silo(1) },
      { ring: new ConsistentHashRing([silo(0), silo(1), silo(2)]), owner: silo(2) },
    ]);
    const grainId = new GrainId("Counter", key);
    let node2: ClusterNode | undefined;

    try {
      expect(await node0.getGrain(ICounter, key).increment(5)).toBe(5);
      membership.addSilo(silo(2));
      node2 = makeNode(silo(2), flaky, {
        recovery: { maxAttempts: 2, backoffMs: 1_000, retryMs: 1_000 },
        activationOptions: { logger },
      });
      const started = node2.start();
      node0.updateView();
      node1.updateView();

      // Pump microtasks and advance the fake clock in lockstep so the in-pass
      // retries' backoff elapses: both attempts fail and the pull exhausts.
      for (let i = 0; i < 8; i++) {
        await Promise.resolve();
        await Promise.resolve();
        time.advance(1_000);
      }
      await started;
      expect(node2.directoryRecoveryStats().exhausted).toBe(1);
      expect(warnings).toHaveLength(1);
      expect(node1.pendingHandoffCount()).toBe(1); // still retained at the source

      // The re-arm runs on its own backoff and gets the range through after all.
      for (let i = 0; i < 8; i++) {
        await Promise.resolve();
        await Promise.resolve();
        time.advance(1_000);
      }
      await settle();
      expect(node1.pendingHandoffCount()).toBe(0);
      expect(await node2.getGrain(ICounter, key).increment(2)).toBe(7);
      expect(node2.isActive(grainId)).toBe(false);
      // Counted per exhausted source, not per re-arm.
      expect(node2.directoryRecoveryStats().exhausted).toBe(1);
      await node2.stop();
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});

describe("recovery ACK identity (a late ACK for an entry that has been replaced)", () => {
  beforeEach(() => undefined);

  it("does not delete a newer entry registered for the same grain in the meantime", async () => {
    // The ACK names what the puller adopted, but the source deleted BY GRAIN ID:
    // a delayed ACK deleted whatever currently sat under that key — including an
    // entry registered since, which the puller never saw and which exists nowhere
    // else once it is gone.
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    // Hold the pull's RESPONSE at the puller: silo-2 has been served silo-1's
    // entry, but has neither adopted nor ACKed it yet.
    const gate = new GatedTransport(
      new InProcessTransport(network, CLUSTER),
      silo(1),
      (message) => message.direction === "response",
    );
    const makeNode = (
      local: SiloAddress,
      transport: Transport = new InProcessTransport(network, CLUSTER),
    ) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport,
        random: () => 0,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };

    const node0 = makeNode(silo(0));
    const node1 = makeNode(silo(1));
    await node0.start();
    await node1.start();

    // silo-1 owns K's entry at two silos; silo-2 owns the range once it joins.
    const ring3 = new ConsistentHashRing([silo(0), silo(1), silo(2)]);
    const key = counterKeyOwnedBy(ring3, silo(2));
    const grainId = new GrainId("Counter", key);
    let node2: ClusterNode | undefined;

    try {
      expect(await node0.getGrain(ICounter, key).increment(5)).toBe(5);
      expect(node1.partition.lookup(grainId)?.silo.equals(silo(0))).toBe(true);

      // silo-2 joins: silo-1 hands its entry off to the snapshot, and answers
      // silo-2's join pull with it — into the parked response.
      membership.addSilo(silo(2));
      node2 = makeNode(silo(2), gate);
      const started = node2.start();
      node0.updateView();
      node1.updateView();
      await started;
      await settle();
      expect(node1.pendingHandoffCount()).toBe(1);

      // A fresh registration for the same grain reaches silo-1's partition (a
      // registration that raced the view change), and the next view change hands
      // it off too — the newer entry replaces the served one under the same key.
      node1.partition.register({ grainId, silo: silo(0), activationId: "activation-2" });
      node1.updateView();
      expect(node1.pendingHandoffCount()).toBe(1);

      // The response is finally delivered: silo-2 adopts the entry it was served
      // and ACKs exactly that one.
      gate.release();
      await settle();

      // That ACK names an entry silo-1 no longer holds, so it must leave the newer
      // one alone — the newer entry is the only copy of that pointer left. (That
      // an ACK which DOES still match deletes is the first test in this file.)
      expect(node1.pendingHandoffCount()).toBe(1);
      expect(node2.isActive(grainId)).toBe(false);
    } finally {
      await node2?.stop();
      await node1.stop();
      await node0.stop();
    }
  });
});
