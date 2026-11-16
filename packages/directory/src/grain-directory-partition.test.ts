import { describe, expect, it } from "vitest";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger, LogFields } from "@thresh/core/logger";
import type { MembershipSnapshot, SiloStatus } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { DirectoryView } from "@thresh/directory/directory-view";
import {
  GrainDirectoryPartition,
  type ActivationsRequest,
  type DirectoryResult,
  type PartitionPeers,
  type SnapshotRequest,
} from "@thresh/directory/grain-directory-partition";
import { rangesContain } from "@thresh/directory/ring-range";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const S0 = silo(0);
const S1 = silo(1);
const S2 = silo(2);

function view(version: number, members: Array<[SiloAddress, SiloStatus]>): DirectoryView {
  const snapshot: MembershipSnapshot = {
    version,
    silos: members.map(([address, status]) => ({ address, status })),
  };
  return DirectoryView.of(snapshot);
}

/** A grain whose hash `owner` owns in `v` (and, if given, `alsoIn` too). */
function grainOwnedBy(v: DirectoryView, owner: SiloAddress, from = 0): GrainId {
  for (let i = from; ; i++) {
    const id = new GrainId("Counter", `k-${i}`);
    if (v.owns(owner, id.getUniformHashCode())) return id;
  }
}

function grainMoving(moves: ReadonlyArray<{ view: DirectoryView; owner: SiloAddress }>): GrainId {
  for (let i = 0; ; i++) {
    const id = new GrainId("Counter", `m-${i}`);
    if (moves.every(({ view: v, owner }) => v.owns(owner, id.getUniformHashCode()))) return id;
  }
}

const addr = (
  grainId: GrainId,
  host: SiloAddress,
  activationId = `act-${grainId.key}`,
): GrainAddress => ({
  grainId,
  silo: host,
  activationId,
});

interface Gate {
  promise: Promise<void>;
  open: () => void;
}
function gate(): Gate {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
}

/** Drain microtasks so in-memory peer calls settle. */
async function flush(): Promise<void> {
  for (let i = 0; i < 50; i++) await Promise.resolve();
}

class RecordingLogger implements Logger {
  readonly records: Array<{ level: string; message: string; fields?: LogFields }> = [];
  debug(message: string, fields?: LogFields): void {
    this.records.push({ level: "debug", message, ...(fields ? { fields } : {}) });
  }
  info(message: string, fields?: LogFields): void {
    this.records.push({ level: "info", message, ...(fields ? { fields } : {}) });
  }
  warn(message: string, fields?: LogFields): void {
    this.records.push({ level: "warn", message, ...(fields ? { fields } : {}) });
  }
  error(message: string, fields?: LogFields): void {
    this.records.push({ level: "error", message, ...(fields ? { fields } : {}) });
  }
}

/**
 * Silos in memory, each with a partition and the activations it hosts. The peers interface is
 * the only boundary faked: a call reaches the other partition's serving method directly, or
 * fails the way the harness is told to (unreachable rejects; gated waits).
 */
class Harness {
  readonly time = new FakeTimeProvider();
  readonly logger = new RecordingLogger();
  latest: DirectoryView = DirectoryView.initial;
  private readonly nodes = new Map<string, HarnessNode>();

  add(address: SiloAddress, options: { recoveryPatienceMs?: number } = {}): HarnessNode {
    const node = new HarnessNode(this, address, options);
    this.nodes.set(address.toString(), node);
    return node;
  }

  node(address: SiloAddress): HarnessNode {
    const node = this.nodes.get(address.toString());
    if (node === undefined) throw new Error(`no node ${address.toString()}`);
    return node;
  }

  /** Publish `v` and apply it on `to` (every node by default). */
  publish(
    v: DirectoryView,
    to: readonly SiloAddress[] = [...this.nodes.values()].map((n) => n.address),
  ): void {
    this.latest = v;
    for (const address of to) this.node(address).partition.applyView(v);
  }
}

class HarnessNode {
  readonly partition: GrainDirectoryPartition;
  activations: GrainAddress[] = [];
  unreachable = false;
  /** A held watch: refreshing brings nothing until the harness publishes to this node. */
  held = false;
  snapshotGate: Gate | undefined;
  recoveryGate: Gate | undefined;
  snapshotAnswer: ((request: SnapshotRequest) => unknown) | undefined;
  readonly recoveryRequests: Array<ActivationsRequest & { from: SiloAddress }> = [];

  constructor(
    private readonly harness: Harness,
    readonly address: SiloAddress,
    options: { recoveryPatienceMs?: number },
  ) {
    const peers: PartitionPeers = {
      getSnapshot: async (owner, request) => {
        const target = harness.node(owner);
        if (target.unreachable) throw new Error(`${owner.podName} unreachable`);
        if (target.snapshotGate !== undefined) await target.snapshotGate.promise;
        if (target.snapshotAnswer !== undefined) {
          return target.snapshotAnswer(request) as GrainAddress[] | undefined;
        }
        return target.partition.serveSnapshot(request);
      },
      acknowledgeSnapshot: async (owner, pair) => {
        harness.node(owner).partition.acknowledgeSnapshot(address, pair);
      },
      recoverActivations: async (member, request) => {
        const target = harness.node(member);
        if (target.unreachable) throw new Error(`${member.podName} unreachable`);
        target.recoveryRequests.push({ ...request, from: address });
        if (target.recoveryGate !== undefined) await target.recoveryGate.promise;
        return target.partition.answerRecovery(address, request);
      },
      registeredActivations: async (member, request) => {
        const target = harness.node(member);
        if (target.unreachable) throw new Error(`${member.podName} unreachable`);
        return target.partition.answerRegistered(request);
      },
    };
    this.partition = new GrainDirectoryPartition({
      local: address,
      peers,
      activations: {
        registeredIn: (ranges) =>
          this.activations.filter((a) => rangesContain(ranges, a.grainId.getUniformHashCode())),
      },
      refreshView: async (version) => {
        if (!this.held && harness.latest.version >= version)
          this.partition.applyView(harness.latest);
      },
      time: harness.time,
      logger: harness.logger,
      ...(options.recoveryPatienceMs !== undefined
        ? { recoveryPatienceMs: options.recoveryPatienceMs }
        : {}),
    });
  }
}

function ok<T>(result: DirectoryResult<T>): T {
  if (result.kind !== "ok") throw new Error(`expected a result, got refresh to ${result.version}`);
  return result.value;
}

/** A promise's state after the microtasks drain, without awaiting it. */
async function stateOf(promise: Promise<unknown>): Promise<"pending" | "settled"> {
  let settled = false;
  void promise.then(
    () => (settled = true),
    () => (settled = true),
  );
  await flush();
  return settled ? "settled" : "pending";
}

describe("GrainDirectoryPartition: requests", () => {
  it("registers by compare-and-swap, stamping the entry with the owner's view version", async () => {
    const h = new Harness();
    const p = h.add(S0).partition;
    const v1 = view(5, [[S0, "active"]]);
    h.publish(v1);
    await p.settled();
    const g = grainOwnedBy(v1, S0);
    const first = ok(await p.register(5, addr(g, S0, "a1")));
    expect(first.version).toBe(5);
    const second = ok(await p.register(5, addr(g, S0, "a2")));
    expect(second.activationId).toBe("a1");
    const replaced = ok(await p.register(5, addr(g, S0, "a2"), first));
    expect(replaced.activationId).toBe("a2");
    expect(ok(await p.lookup(5, g))?.activationId).toBe("a2");
    ok(await p.unregister(5, addr(g, S0, "a1")));
    expect(ok(await p.lookup(5, g))?.activationId).toBe("a2");
    ok(await p.unregister(5, addr(g, S0, "a2")));
    expect(ok(await p.lookup(5, g))).toBeUndefined();
  });

  it("catches up when the caller is ahead instead of rejecting", async () => {
    const h = new Harness();
    const p = h.add(S0).partition;
    h.add(S1);
    const v1 = view(1, [[S0, "active"]]);
    h.publish(v1);
    await p.settled();
    // The cluster moves on; S0 has not applied it yet.
    const v2 = view(2, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    h.latest = v2;
    const g = grainOwnedBy(v2, S0);
    const result = await p.register(2, addr(g, S0));
    expect(result.kind).toBe("ok");
    expect(p.view.version).toBe(2);
  });

  it("tells a caller that is behind to refresh, with its own version", async () => {
    const h = new Harness();
    const p0 = h.add(S0).partition;
    h.add(S1);
    h.publish(view(1, [[S0, "active"]]));
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    h.publish(v2);
    await p0.settled();
    const g = grainOwnedBy(v2, S1);
    expect(await p0.lookup(1, g)).toEqual({ kind: "refresh", version: 2 });
  });

  it("checks ownership when the versions are equal", async () => {
    const h = new Harness();
    const p0 = h.add(S0).partition;
    h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
    ]);
    h.publish(v1);
    await p0.settled();
    const g = grainOwnedBy(v1, S1);
    expect(await p0.lookup(1, g)).toEqual({ kind: "refresh", version: 1 });
    expect(await p0.register(1, addr(g, S1))).toEqual({ kind: "refresh", version: 1 });
    expect(await p0.unregister(1, addr(g, S1))).toEqual({ kind: "refresh", version: 1 });
  });

  it("answers a lookup with nothing once the view that dropped the host is applied", async () => {
    const h = new Harness();
    const p0 = h.add(S0).partition;
    h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "draining"],
    ]);
    h.publish(v1);
    await p0.settled();
    const g = grainOwnedBy(v1, S0);
    ok(await p0.register(1, addr(g, S1, "on-s1")));
    expect(ok(await p0.lookup(1, g))?.silo.equals(S1)).toBe(true);
    h.publish(view(2, [[S0, "active"]]), [S0]);
    expect(ok(await p0.lookup(2, g))).toBeUndefined();
    expect(p0.entryFor(g)).toBeUndefined();
  });
  it("hides a held entry whose host is not a member from a lookup, without removing it (Orleans IsSiloDead)", async () => {
    const h = new Harness();
    const p0 = h.add(S0).partition;
    const v1 = view(1, [[S0, "active"]]);
    h.publish(v1);
    await p0.settled();
    const g = grainOwnedBy(v1, S0);
    // An entry for a host this view does not list: applyView never saw it as a member, so it
    // did not delete it, and only the lookup stands between the entry and the caller.
    ok(await p0.register(1, addr(g, silo(9), "on-a-stranger")));
    expect(p0.entryFor(g)?.silo.equals(silo(9))).toBe(true);
    expect(ok(await p0.lookup(1, g))).toBeUndefined();
    // A host that is a member is returned.
    const other = grainOwnedBy(v1, S0, 10_000);
    ok(await p0.register(1, addr(other, S0, "here")));
    expect(ok(await p0.lookup(1, other))?.activationId).toBe("here");
  });

  it("lets a registration replace an entry whose host is not a member (Orleans RegisterCore)", async () => {
    const h = new Harness();
    const p0 = h.add(S0).partition;
    const v1 = view(1, [[S0, "active"]]);
    h.publish(v1);
    await p0.settled();
    const g = grainOwnedBy(v1, S0);
    // Registered for a silo this partition has never seen as a member.
    ok(await p0.register(1, addr(g, silo(9), "on-a-stranger")));
    const winner = ok(await p0.register(1, addr(g, S0, "here")));
    expect(winner.activationId).toBe("here");
  });
});

describe("GrainDirectoryPartition: view changes", () => {
  it("hands a lost range to its new owner through the snapshot for exactly that view change", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    h.publish(v1);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    // Hosted on S0, which owns it at v1.
    ok(await n0.partition.register(1, addr(g, S0)));
    h.publish(v2);
    await n0.partition.settled();
    await n1.partition.settled();
    await flush();

    expect(ok(await n1.partition.lookup(2, g))?.silo.equals(S0)).toBe(true);
    expect(n0.partition.entryFor(g)).toBeUndefined();
    // Acknowledged: the previous owner no longer holds it.
    expect(n0.partition.snapshotCount).toBe(0);
    expect(n1.partition.stats.transferred).toBe(1);
    // A snapshot, not a recovery: no member was asked for its activations at v2.
    expect(n0.recoveryRequests.filter((r) => r.version === 2)).toEqual([]);
  });

  it("serves a snapshot only to a silo that moved between the same two views", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    h.publish(v1);
    await n0.partition.settled();
    await n1.partition.settled();
    // S0 gives the range to S1 at v2 and holds the snapshot for (1, 2).
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    ok(await n0.partition.register(1, addr(g, S0, "stale")));
    h.publish(v2, [S0]);
    await n0.partition.settled();
    expect(n0.partition.snapshotCount).toBe(1);

    const ranges = v2.rangesOf(S1);
    const served = await n0.partition.serveSnapshot({ previousVersion: 1, version: 2, ranges });
    expect(served?.map((e) => e.grainId.toString())).toEqual([g.toString()]);
    // The same ranges at the same version, from a view S0 did not move from: nothing is served.
    expect(
      await n0.partition.serveSnapshot({ previousVersion: 0, version: 2, ranges }),
    ).toBeUndefined();
    // The right view to a view S0 did not move to: nothing is served.
    h.publish(
      view(3, [
        [S0, "active"],
        [S1, "active"],
      ]),
      [S0],
    );
    expect(
      await n0.partition.serveSnapshot({ previousVersion: 1, version: 3, ranges }),
    ).toBeUndefined();
  });

  it("recovers, and ignores the snapshot it was not owed, when the new owner skipped a view", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    h.publish(v1);
    await n0.partition.settled();
    await n1.partition.settled();
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    // The snapshot S0 takes at v2 holds an entry that is no longer true: the grain lives on S1.
    ok(await n0.partition.register(1, addr(g, S0, "stale")));
    h.publish(v2, [S0]);
    await n0.partition.settled();
    expect(n0.partition.snapshotCount).toBe(1);
    n1.activations = [{ ...addr(g, S1, "live"), version: 1 }];

    // S1 skips v2 and applies v3, an identical membership: it moves from v1 to v3, a pair S0
    // holds no snapshot for, although S0's snapshot for (1, 2) covers the same range.
    const v3 = view(3, [
      [S0, "active"],
      [S1, "active"],
    ]);
    h.publish(v3, [S1]);
    await n1.partition.settled();
    await n0.partition.settled();
    await flush();

    expect(ok(await n1.partition.lookup(3, g))?.activationId).toBe("live");
    expect(n1.partition.stats.transferred).toBe(0);
    expect(n0.recoveryRequests.filter((r) => r.from.equals(S1)).map((r) => r.version)).toEqual([3]);
  });

  it("recovers when the new owner's first view leaves no pair it shares with the previous owner", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    // S1's first view is v2: there is no pair it shares with S0.
    n1.held = true;
    h.publish(v1, [S0]);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    ok(await n0.partition.register(1, addr(g, S0)));
    n0.activations = [{ ...addr(g, S0), version: 1 }];
    h.publish(v2);
    await n1.partition.settled();
    await n0.partition.settled();
    await flush();

    expect(ok(await n1.partition.lookup(2, g))?.silo.equals(S0)).toBe(true);
    expect(n0.recoveryRequests.filter((r) => r.from.equals(S1)).map((r) => r.version)).toEqual([2]);
    // It came for nothing from S0's snapshot, so S0 dropped it once S1 started recovery.
    expect(n0.partition.snapshotCount).toBe(0);
    expect(n0.partition.recoveryVersion).toBe(2);
  });

  it("deletes the entries of members that left the view", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
    ]);
    h.publish(v1);
    await n0.partition.settled();
    const g = grainOwnedBy(v1, S0);
    ok(await n0.partition.register(1, addr(g, S1)));
    h.publish(view(2, [[S0, "active"]]), [S0]);
    expect(n0.partition.entryFor(g)).toBeUndefined();
  });

  it("waits on a lock at or below a request's version, and not on one above it", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [[S0, "active"]]);
    h.publish(v1);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    ok(await n0.partition.register(1, addr(g, S0)));
    // S0 gives the range to S1 at v2, then takes it back at v3 while S1 is held up.
    h.publish(v2, [S0]);
    await n0.partition.settled();
    expect(n0.partition.snapshotCount).toBe(1);
    const v3 = view(3, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    n1.snapshotGate = gate();
    h.publish(v3, [S0]);
    // S0 is acquiring the range at v3 from S1 (which has not applied v2: it holds nothing yet).
    const lookup = n0.partition.lookup(3, g);
    expect(await stateOf(lookup)).toBe("pending");
    // S1's request for the v1 -> v2 snapshot is below that lock and is served at once.
    const served = await n0.partition.serveSnapshot({
      previousVersion: 1,
      version: 2,
      ranges: v2.rangesOf(S1),
    });
    expect(served?.map((e) => e.grainId.toString())).toEqual([g.toString()]);
    n1.snapshotGate.open();
    await n0.partition.settled();
    expect(await stateOf(lookup)).toBe("settled");
  });

  it("keeps the range locked and retries when acquisition fails unexpectedly, never unlocking it empty", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    h.publish(v1);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    ok(await n0.partition.register(1, addr(g, S0)));
    n0.activations = [{ ...addr(g, S0), version: 1 }];
    // The previous owner answers with something that is not a snapshot.
    n0.snapshotAnswer = () => 42;
    h.publish(v2);
    const lookup = n1.partition.lookup(2, g);
    expect(await stateOf(lookup)).toBe("pending");
    expect(n1.partition.lockedRanges.length).toBeGreaterThan(0);
    expect(h.logger.records.some((r) => r.level === "error")).toBe(true);

    h.time.advance(1_000);
    await n1.partition.settled();
    // Retried as a recovery, which found the activation on its host.
    expect(ok(await lookup)?.silo.equals(S0)).toBe(true);
  });
});

describe("GrainDirectoryPartition: handing over", () => {
  /** S0 hosts and owns `count` grains at v1, then leaves at v2; `others` are the other members. */
  async function leaving(
    h: Harness,
    others: Array<[SiloAddress, SiloStatus]>,
    count = 10,
  ): Promise<{ v1: DirectoryView; v2: DirectoryView }> {
    const n0 = h.add(S0);
    for (const [address] of others) h.add(address);
    const v1 = view(1, [[S0, "active"], ...others]);
    h.publish(v1);
    for (let i = 0, registered = 0; registered < count; i++) {
      const g = new GrainId("Counter", `leaving-${i}`);
      if (!v1.owns(S0, g.getUniformHashCode())) continue;
      ok(await n0.partition.register(1, addr(g, S0)));
      registered++;
    }
    const v2 = view(2, [[S0, "draining"], ...others]);
    // Only the leaver applies it: no partner has collected yet.
    h.publish(v2, [S0]);
    await n0.partition.settled();
    expect(n0.partition.snapshotCount).toBeGreaterThan(0);
    return { v1, v2 };
  }

  it("does not wait on a snapshot whose only partners are not active, and still serves it", async () => {
    const h = new Harness();
    // S1 is a dead pod still listed not ready: with S0 leaving it is the ring by the cold-start
    // fallback, so it is the only partner of everything S0 gives up.
    const { v1, v2 } = await leaving(h, [[S1, "joining"]]);
    const n0 = h.node(S0);
    expect(v2.ringSilos).toEqual([S1]);

    // Settled with the clock never advanced: it did not wait for its bound.
    expect(await stateOf(n0.partition.handedOver(10_000))).toBe("settled");
    // Still held, and still served if the partner does come for it.
    expect(n0.partition.snapshotCount).toBe(1);
    const served = await n0.partition.serveSnapshot({
      previousVersion: v1.version,
      version: v2.version,
      ranges: v2.rangesOf(S1),
    });
    expect(served).toHaveLength(10);
  });

  for (const outcome of ["collects", "leaves", "recovers"] as const) {
    it(`waits on a snapshot with an active partner until that partner ${outcome}`, async () => {
      const h = new Harness();
      const { v2 } = await leaving(h, [
        [S1, "active"],
        [S2, "joining"],
      ]);
      const n0 = h.node(S0);
      expect(v2.ringSilos).toEqual([S1]);
      const handedOver = n0.partition.handedOver(10_000);
      expect(await stateOf(handedOver)).toBe("pending");

      if (outcome === "collects") {
        h.publish(v2, [S1]);
        await h.node(S1).partition.settled();
      } else if (outcome === "leaves") {
        h.publish(
          view(3, [
            [S0, "draining"],
            [S2, "joining"],
          ]),
          [S0],
        );
      } else {
        n0.partition.answerRecovery(S1, { version: v2.version, ranges: v2.rangesOf(S1) });
      }
      expect(await stateOf(handedOver)).toBe("settled");
      expect(n0.partition.snapshotCount).toBe(0);
    });
  }

  it("waits on a snapshot whose partner turns active, and not on one whose partner stops being", async () => {
    const h = new Harness();
    await leaving(h, [[S1, "joining"]]);
    const n0 = h.node(S0);
    expect(await stateOf(n0.partition.handedOver(10_000))).toBe("settled");

    // The partner turns ready without collecting: the snapshot holds a leaver up again.
    h.publish(
      view(3, [
        [S0, "draining"],
        [S1, "active"],
      ]),
      [S0],
    );
    await n0.partition.settled();
    const handedOver = n0.partition.handedOver(10_000);
    expect(await stateOf(handedOver)).toBe("pending");

    // It turns not ready again: the pending wait ends with the snapshot still held.
    h.publish(
      view(4, [
        [S0, "draining"],
        [S1, "joining"],
      ]),
      [S0],
    );
    expect(await stateOf(handedOver)).toBe("settled");
    expect(n0.partition.snapshotCount).toBe(1);
  });
});

describe("GrainDirectoryPartition: a request that outlives its range", () => {
  it("answers refresh rather than writing into a range the view moved away during its wait", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "joining"],
    ]);
    // S0's first view: it recovers its ranges, and S1 is slow to answer.
    n1.recoveryGate = gate();
    h.publish(v1, [S0]);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S0 },
      { view: v2, owner: S1 },
    ]);
    const registering = n0.partition.register(1, addr(g, S0));
    expect(await stateOf(registering)).toBe("pending");
    // The range moves to S1 while the registration waits on the lock.
    h.publish(v2, [S0]);
    n1.recoveryGate.open();
    expect(await registering).toEqual({ kind: "refresh", version: 2 });
    await n0.partition.settled();
    expect(n0.partition.entryFor(g)).toBeUndefined();
  });
});

describe("GrainDirectoryPartition: recovery", () => {
  it("keeps the range locked while a member does not answer, and unlocks once it leaves the view", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    h.add(S2);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
      [S2, "active"],
    ]);
    h.publish(v1);
    await n0.partition.settled();
    // S2 dies; it is still listed, not ready, and cannot be reached.
    h.node(S2).unreachable = true;
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
      [S2, "joining"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S2 },
      { view: v2, owner: S0 },
    ]);
    // The grain itself lives on S1.
    n1.activations = [{ ...addr(g, S1), version: 1 }];
    h.publish(v2, [S0, S1]);
    const lookup = n0.partition.lookup(2, g);
    for (let i = 0; i < 5; i++) {
      h.time.advance(2_000);
      await flush();
    }
    expect(await stateOf(lookup)).toBe("pending");

    // Kubernetes removes the pod: S2 stops being asked, and recovery completes.
    h.publish(
      view(3, [
        [S0, "active"],
        [S1, "active"],
      ]),
      [S0, S1],
    );
    h.time.advance(2_000);
    await n0.partition.settled();
    expect(ok(await lookup)?.silo.equals(S1)).toBe(true);
  });

  it("unlocks the range when the recovery patience elapses, logging and counting it", async () => {
    const h = new Harness();
    const n0 = h.add(S0, { recoveryPatienceMs: 5_000 });
    h.add(S1);
    h.add(S2);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
      [S2, "active"],
    ]);
    h.publish(v1);
    await n0.partition.settled();
    h.node(S2).unreachable = true;
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
      [S2, "joining"],
    ]);
    const g = grainMoving([
      { view: v1, owner: S2 },
      { view: v2, owner: S0 },
    ]);
    h.publish(v2, [S0, S1]);
    const lookup = n0.partition.lookup(2, g);
    h.time.advance(4_000);
    expect(await stateOf(lookup)).toBe("pending");
    h.time.advance(1_500);
    await n0.partition.settled();
    expect(ok(await lookup)).toBeUndefined();
    expect(n0.partition.stats.patienceExpired).toBe(1);
    expect(
      h.logger.records.some(
        (r) => r.level === "error" && r.message.includes("recovery patience elapsed"),
      ),
    ).toBe(true);
  });

  it("asks every member, itself included, and takes a refused member's empty answer", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "joining"],
      [S1, "joining"],
    ]);
    const g0 = grainOwnedBy(v1, S0);
    // S0's own activation in its own range, and one hosted on S1.
    n0.activations = [{ ...addr(g0, S0), version: 1 }];
    const g1 = grainOwnedBy(v1, S0, 10_000);
    n1.activations = [{ ...addr(g1, S1), version: 1 }];
    h.publish(v1);
    await n0.partition.settled();
    expect(ok(await n0.partition.lookup(1, g0))?.silo.equals(S0)).toBe(true);
    expect(ok(await n0.partition.lookup(1, g1))?.silo.equals(S1)).toBe(true);
    // Its first view: it asked itself as well as S1.
    expect(n0.recoveryRequests.filter((r) => r.from.equals(S0))).toHaveLength(1);
    expect(n1.recoveryRequests.filter((r) => r.from.equals(S0))).toHaveLength(1);
  });
});

describe("GrainDirectoryPartition: integrity", () => {
  it("reports an activation with no entry at its owner, and passes once it has one", async () => {
    const h = new Harness();
    const n0 = h.add(S0);
    const n1 = h.add(S1);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
    ]);
    h.publish(v1);
    await n0.partition.settled();
    await n1.partition.settled();
    const g = grainOwnedBy(v1, S0);
    n1.activations = [{ ...addr(g, S1), version: 1 }];
    const violations = await n0.partition.checkIntegrity();
    expect(violations.map((v) => v.kind)).toEqual(["missing"]);
    const registered = ok(await n0.partition.register(1, addr(g, S1)));
    n1.activations = [registered];
    expect(await n0.partition.checkIntegrity()).toEqual([]);
  });
});
