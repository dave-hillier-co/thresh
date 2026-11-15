import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type {
  DehydrationContext,
  IGrainMigrationParticipant,
  RehydrationContext,
} from "@thresh/core/grain-migration-participant";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import type { Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.recoveryVersion");

@grain()
class CounterGrain extends Grain implements ICounter, IGrainMigrationParticipant {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
  onDehydrate(ctx: DehydrationContext): void {
    ctx.set("count", this.count);
  }
  onRehydrate(ctx: RehydrationContext): void {
    this.count = ctx.get<number>("count") ?? this.count;
  }
}

const CLUSTER = "c1";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const [A, B, C, D] = [silo(0), silo(1), silo(2), silo(3)] as const;

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
  refresh(version: number) {
    return this.shared.refresh(version);
  }
  localSilo() {
    return this.local;
  }
}

const serializer = new MessagePackSerializer();

/** Whether `message` is a directory reply carrying a registration's winner. */
function isRegistrationReply(message: Message): boolean {
  if (message.direction !== "response") return false;
  try {
    const body = serializer.deserialize<{ kind?: string; value?: { activationId?: string } }>(
      message.body,
    );
    return body?.kind === "ok" && typeof body.value?.activationId === "string";
  } catch {
    return false;
  }
}

/**
 * A's transport: the first registration reply from `from` is parked, everything else passes.
 * That is the window the recovery-version rule closes — the owner has taken the registration,
 * and the activation does not exist yet to be reported to a recovering member.
 */
class ParkingTransport implements Transport {
  private parkedReply: (() => void) | undefined;
  private armed = false;
  constructor(
    private readonly inner: Transport,
    private readonly from: SiloAddress,
  ) {}
  arm(): void {
    this.armed = true;
  }
  get parked(): boolean {
    return this.parkedReply !== undefined;
  }
  release(): void {
    const deliver = this.parkedReply;
    this.parkedReply = undefined;
    deliver?.();
  }
  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    return this.inner.listen(address, onMessage, onAccept);
  }
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const gated: MessageHandler | undefined =
      onMessage &&
      ((message, peer, connection) => {
        if (this.armed && peer.equals(this.from) && isRegistrationReply(message)) {
          this.armed = false;
          this.parkedReply = () => void onMessage(message, peer, connection);
          return undefined;
        }
        return onMessage(message, peer, connection);
      });
    return this.inner.connect(to, preamble, gated);
  }
}

/** The first key owned by `from` at three silos and by `to` once B has left the ring. */
function keyMoving(from: SiloAddress, to: SiloAddress): string {
  for (let i = 0; ; i++) {
    const grainId = new GrainId("Counter", `g-${i}`);
    if (
      new ConsistentHashRing([A, B, C]).ownerOf(grainId).equals(from) &&
      new ConsistentHashRing([A, C]).ownerOf(grainId).equals(to)
    ) {
      return `g-${i}`;
    }
  }
}

async function until(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !condition(); i++) await new Promise((r) => setTimeout(r, 1));
  if (!condition()) throw new Error("condition not reached");
}

/**
 * A registration whose owner then hands its range on without the entry: A claims grain `g`,
 * whose owner is B; B takes the registration, and A's reply is held. B leaves the ring, and A —
 * which has applied neither the view B left in nor any other since — moves between a pair of
 * views no snapshot was made for, so it recovers B's range from the hosts. The activation of `g`
 * does not exist on A yet, so A reports nothing for it; but A's recovery version rises.
 *
 * Two mechanisms close this window, and each closes it alone: the directory sends again an
 * operation during which the recovery version rose (Orleans `InvokeAsync`), and the dispatcher
 * confirms again a registration stamped below it before creating the activation (Orleans
 * `ActivationData.RegisterAsync`). With both disabled, A activates `g` beside C's activation.
 */
async function registrationOutlivedByRecovery() {
  const network = new InProcessNetwork();
  const membership = new StaticMembershipService(A, [A, B, C]);
  const parking = new ParkingTransport(new InProcessTransport(network, CLUSTER), B);
  const make = (local: SiloAddress, transport: Transport) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    node.registerGrain(CounterGrain, { interfaces: [ICounter] });
    return node;
  };
  const a = make(A, parking);
  const b = make(B, new InProcessTransport(network, CLUSTER));
  const c = make(C, new InProcessTransport(network, CLUSTER));
  for (const node of [a, b, c]) await node.start();
  for (const node of [a, b, c]) await node.recoverySettled();

  // `g`: B's at three silos, A's once B has left the ring.
  const key = keyMoving(B, A);
  const grainId = new GrainId("Counter", key);

  parking.arm();
  const callFromA = a.getGrain(ICounter, key).increment(1);
  await until(() => parking.parked);
  expect(b.partition.entryFor(grainId)?.silo.equals(A)).toBe(true);

  // B leaves the ring (v2), and a joining silo with no process yet is listed (v3). B and C
  // apply both; A applies only the last.
  membership.setStatus(B, "draining");
  b.updateView();
  c.updateView();
  membership.addSilo(D, "joining");
  b.updateView();
  c.updateView();
  a.updateView();
  await a.recoverySettled();
  expect(a.partition.recoveryVersion).toBe(a.appliedViewVersion);
  expect(a.partition.entryFor(grainId)).toBeUndefined();

  return { a, b, c, parking, key, grainId, callFromA, stop: () => stopAll([a, b, c]) };
}

async function stopAll(nodes: ClusterNode[]): Promise<void> {
  for (const node of nodes) await node.stop();
}

describe("the recovery-version rule (DistributedGrainDirectory._recoveryMembershipVersion)", () => {
  it("re-confirms a registration that completed below the silo's recovery version", async () => {
    const { a, b, c, parking, grainId, callFromA, stop } = await registrationOutlivedByRecovery();
    try {
      parking.release();
      expect(await callFromA).toBe(1);
      // Confirmed again at the owner that recovered the range, at its view.
      const entry = a.partition.entryFor(grainId);
      expect(entry?.silo.equals(A)).toBe(true);
      expect(entry?.version).toBe(a.appliedViewVersion);
      for (const node of [a, b, c]) expect(await node.checkDirectoryIntegrity()).toEqual([]);
    } finally {
      await stop();
    }
  });

  it("gives way to the activation recovery installed meanwhile, and forwards the call to it", async () => {
    const { a, b, c, parking, key, grainId, callFromA, stop } =
      await registrationOutlivedByRecovery();
    try {
      // C activates the grain at its owner, which no longer holds A's registration.
      expect(await c.getGrain(ICounter, key).increment(10)).toBe(10);
      expect(a.partition.entryFor(grainId)?.silo.equals(C)).toBe(true);

      // A's held registration arrives: it is confirmed again, loses to C's entry, and A's call
      // is answered by C's activation.
      parking.release();
      expect(await callFromA).toBe(11);
      expect(a.isActive(grainId)).toBe(false);
      expect(c.isActive(grainId)).toBe(true);
      for (const node of [a, b, c]) expect(await node.checkDirectoryIntegrity()).toEqual([]);
    } finally {
      await stop();
    }
  });
});

const E = silo(4);

/** The first key owned by B with all four silos active, and by A once B has left the ring. */
function keyMovingFromBToA(): string {
  for (let i = 0; ; i++) {
    const grainId = new GrainId("Counter", `m-${i}`);
    if (
      new ConsistentHashRing([A, B, C, E]).ownerOf(grainId).equals(B) &&
      new ConsistentHashRing([A, C, E]).ownerOf(grainId).equals(A)
    ) {
      return `m-${i}`;
    }
  }
}

/**
 * A migration whose registration is outlived by a recovery: `g`, hosted on C with count 5, is
 * migrated to A. A registers the migrated activation at the owner, B, which takes it, and B's
 * reply is held. B leaves the ring and a joiner is listed; A, which has applied neither view
 * before, recovers B's range from the hosts, so its recovery version rises above the stamp of
 * the registration it is waiting for. C's activation is deactivating, so it reports nothing.
 */
async function migrationOutlivedByRecovery() {
  const network = new InProcessNetwork();
  const membership = new StaticMembershipService(A, [A, B, C, E]);
  const parking = new ParkingTransport(new InProcessTransport(network, CLUSTER), B);
  const make = (local: SiloAddress, transport: Transport) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    node.registerGrain(CounterGrain, { interfaces: [ICounter] });
    return node;
  };
  const a = make(A, parking);
  const b = make(B, new InProcessTransport(network, CLUSTER));
  const c = make(C, new InProcessTransport(network, CLUSTER));
  const e = make(E, new InProcessTransport(network, CLUSTER));
  const nodes = [a, b, c, e];
  for (const node of nodes) await node.start();
  for (const node of nodes) await node.recoverySettled();

  const key = keyMovingFromBToA();
  const grainId = new GrainId("Counter", key);
  expect(await c.getGrain(ICounter, key).increment(5)).toBe(5);
  expect(c.isActive(grainId)).toBe(true);
  expect(b.partition.entryFor(grainId)?.silo.equals(C)).toBe(true);

  parking.arm();
  const migrating = c.migrateGrain(grainId, A);
  await until(() => parking.parked);
  expect(b.partition.entryFor(grainId)?.silo.equals(A)).toBe(true);

  membership.setStatus(B, "draining");
  for (const node of [b, c, e]) node.updateView();
  membership.addSilo(D, "joining");
  for (const node of [b, c, e]) node.updateView();
  a.updateView();
  await a.recoverySettled();
  expect(a.partition.recoveryVersion).toBe(a.appliedViewVersion);
  expect(a.partition.entryFor(grainId)).toBeUndefined();

  return { a, b, c, e, nodes, parking, key, grainId, migrating, stop: () => stopAll(nodes) };
}

describe("the recovery-version rule on a migration target (ActivationData.RegisterAsync)", () => {
  it("confirms a migrated activation's registration again when it completed below the recovery version", async () => {
    const { a, nodes, parking, key, grainId, migrating, stop } =
      await migrationOutlivedByRecovery();
    try {
      parking.release();
      expect(await migrating).toBe(true);
      // The entry was stamped by B at the old view; it is confirmed again at the owner that
      // recovered the range, at its view.
      const entry = a.partition.entryFor(grainId);
      expect(entry?.silo.equals(A)).toBe(true);
      expect(entry?.version).toBe(a.appliedViewVersion);
      expect(await a.getGrain(ICounter, key).increment(1)).toBe(6);
      for (const node of nodes) expect(await node.checkDirectoryIntegrity()).toEqual([]);
    } finally {
      await stop();
    }
  });

  it("deactivates the migrated activation as a duplicate when another activation holds the entry by then", async () => {
    const { a, c, e, nodes, parking, key, grainId, migrating, stop } =
      await migrationOutlivedByRecovery();
    try {
      // E activates the grain at its owner, which no longer holds A's registration.
      expect(await e.getGrain(ICounter, key).increment(10)).toBe(10);
      expect(a.partition.entryFor(grainId)?.silo.equals(E)).toBe(true);

      // A's held registration arrives, is confirmed again and loses to E's entry: the migration
      // is declined, and the state it carried is dropped with it, as Orleans drops it.
      parking.release();
      expect(await migrating).toBe(false);
      expect(a.isActive(grainId)).toBe(false);
      expect(c.isActive(grainId)).toBe(false);
      expect(e.isActive(grainId)).toBe(true);
      expect(await a.getGrain(ICounter, key).increment(1)).toBe(11);
      for (const node of nodes) expect(await node.checkDirectoryIntegrity()).toEqual([]);
    } finally {
      await stop();
    }
  });
});
