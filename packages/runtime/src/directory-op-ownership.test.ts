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
const ICounter = defineGrainInterface<ICounter>("ICounter.opOwnership");

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

/**
 * Wraps a transport so inbound directory requests from `holdFrom` are parked
 * instead of delivered, until `release()` hands them to the real handler. That is
 * what keeps a range recovery in flight for as long as the test needs: every
 * directory operation that waits on that recovery waits with it, which is the
 * yield this file's interleaving lives in.
 */
class HoldingTransport implements Transport {
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
      const isHeld =
        !this.open &&
        message.direction === "request" &&
        message.system === "directory" &&
        from.equals(this.holdFrom);
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

/** First `Counter/*` key every `(ring, owner)` pair agrees on — arranges a deterministic move. */
function counterKeyMovingThrough(
  moves: ReadonlyArray<{ ring: ConsistentHashRing; owner: SiloAddress }>,
): string {
  for (let i = 0; ; i++) {
    const grainId = new GrainId("Counter", `k-${i}`);
    if (moves.every(({ ring, owner }) => ring.ownerOf(grainId).equals(owner))) return `k-${i}`;
  }
}

/**
 * A directory op is decided against the ring, then awaits — recovery on the
 * remote path, the owned-access barrier on the local one — and only then touches
 * the partition. A view change landing in that yield leaves it writing into a
 * partition the ring no longer assigns: nothing re-drains it until the next view
 * change, and a successor that inherits the range finds no entry, so a call
 * misses and builds a second activation of a grain that never stopped running.
 *
 * This file drives the REMOTE path: silo-2 claims a cold activation, so its
 * registration is routed to silo-1 (the range's owner at the time), and silo-1's
 * view moves the range on while that registration waits on silo-1's in-flight
 * recovery.
 */
describe("a remote directory op that outlives its range (check-then-write)", () => {
  beforeEach(() => undefined);

  it("rejects rather than registering into a partition that no longer owns the range", async () => {
    const network = new InProcessNetwork();
    // silo-0 is the lone member to begin with and never applies another view, so
    // its own ring keeps resolving any grain to itself: the call below never
    // needs a directory round trip of its own, which keeps this interleaving
    // about silo-1 and the registration alone.
    const membership = new StaticMembershipService(silo(0), [silo(0)]);
    // silo-1's join recovery pulls from silo-0; parking that pull keeps the
    // recovery in flight for the rest of the test.
    const gate = new HoldingTransport(new InProcessTransport(network, CLUSTER), silo(1));
    const makeNode = (
      local: SiloAddress,
      transport: Transport = new InProcessTransport(network, CLUSTER),
    ) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport,
        // 0.6 of four candidates is the third, silo-2: the activation is claimed
        // there, and silo-2's own ring has yet to learn about silo-3.
        random: () => 0.6,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };

    const node0 = makeNode(silo(0), gate);
    await node0.start();

    membership.addSilo(silo(1));
    const node1 = makeNode(silo(1));
    await node1.start(); // its recovery pull is now parked at silo-0

    // silo-2's node is built while the view still holds two silos, so its ring
    // resolves the grain to silo-1 — and it is then added to the view (and so
    // becomes a placement candidate) without applying that change.
    const node2 = makeNode(silo(2));
    await node2.start();
    membership.addSilo(silo(2));

    // silo-3 joins the view but its node is started later: its own join recovery
    // would pull from the others, and a pull carries the joiner's view version, so
    // starting it now would catch silo-2's view up to four silos before it has
    // routed anything.
    membership.addSilo(silo(3));

    // silo-1 owns the entry at two silos; silo-3 owns the range at four.
    const key = counterKeyMovingThrough([
      { ring: new ConsistentHashRing([silo(0), silo(1)]), owner: silo(1) },
      { ring: new ConsistentHashRing([silo(0), silo(1), silo(2), silo(3)]), owner: silo(3) },
    ]);
    const grainId = new GrainId("Counter", key);
    let node3: ClusterNode | undefined;

    try {
      // The call is placed on silo-2, which claims the activation and registers
      // it with silo-1 — where the registration waits on the parked recovery.
      let completed = false;
      const call = node0.getGrain(ICounter, key).increment(5);
      void call.finally(() => (completed = true));
      await settle();
      expect(completed).toBe(false);
      expect(node1.partition.lookup(grainId)).toBeUndefined();

      // silo-3 comes up — and silo-1 applies the joined view with it (a pull
      // carries the joiner's version, so this happens either way): the range is
      // silo-3's now, and silo-1's parked operation resumes holding the ownership
      // it decided before the view moved.
      node3 = makeNode(silo(3));
      await node3.start();
      node1.updateView();
      gate.release();
      await settle();

      // The call still resolves — silo-2's claim is repaired onto the true owner —
      // but silo-1 must not have registered anything for a range it has lost.
      expect(await call).toBe(5);
      expect(node1.partition.lookup(grainId)).toBeUndefined();
      expect(node3.partition.lookup(grainId)?.silo.equals(silo(2))).toBe(true);

      // And the claim really is the one activation: a later call reaches it.
      expect(await node0.getGrain(ICounter, key).increment(1)).toBe(6);
      expect(node2.isActive(grainId)).toBe(true);
      expect(node3.isActive(grainId)).toBe(false);
    } finally {
      await node3?.stop();
      await node2.stop();
      await node1.stop();
      await node0.stop();
    }
  });
});
