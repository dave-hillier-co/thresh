import { describe, expect, it } from "vitest";
import { newActivationId } from "@thresh/core/activation-id";
import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import type { MembershipSnapshot, SiloStatus } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { DirectoryPeer } from "@thresh/directory/directory-peer";
import { DirectoryView } from "@thresh/directory/directory-view";
import { DistributedGrainDirectory } from "@thresh/directory/distributed-grain-directory";
import {
  GrainDirectoryPartition,
  type PartitionPeers,
} from "@thresh/directory/grain-directory-partition";

const S0 = new SiloAddress("silo-A", "ua", "a:1");
const S1 = new SiloAddress("silo-B", "ub", "b:1");
const S2 = new SiloAddress("silo-C", "uc", "c:1");

function view(version: number, members: Array<[SiloAddress, SiloStatus]>): DirectoryView {
  const snapshot: MembershipSnapshot = {
    version,
    silos: members.map(([address, status]) => ({ address, status })),
  };
  return DirectoryView.of(snapshot);
}

const addr = (grainId: GrainId, silo: SiloAddress): GrainAddress => ({
  grainId,
  silo,
  activationId: newActivationId(),
});

function grainMoving(moves: ReadonlyArray<{ view: DirectoryView; owner: SiloAddress }>): GrainId {
  for (let i = 0; ; i++) {
    const id = new GrainId("Counter", `moved-${i}`);
    if (moves.every(({ view: v, owner }) => v.owns(owner, id.getUniformHashCode()))) return id;
  }
}

async function flush(): Promise<void> {
  for (let i = 0; i < 50; i++) await Promise.resolve();
}

/**
 * Silos with a real partition and directory each; the network between them is a direct call,
 * and each silo's view is whatever the test has delivered to it.
 */
class Cluster {
  readonly time = new FakeTimeProvider();
  latest = DirectoryView.initial;
  readonly partitions = new Map<string, GrainDirectoryPartition>();
  readonly directories = new Map<string, DistributedGrainDirectory>();
  readonly refreshed: Array<{ silo: string; version: number }> = [];
  /** Silos whose refresh delivers nothing (a held watch). */
  readonly held = new Set<string>();
  peerCalls = 0;

  constructor(silos: SiloAddress[]) {
    for (const silo of silos) {
      const peers: PartitionPeers = {
        getSnapshot: async (owner, request) => this.partition(owner).serveSnapshot(request),
        acknowledgeSnapshot: async (owner, pair) =>
          this.partition(owner).acknowledgeSnapshot(silo, pair),
        recoverActivations: async (member, request) =>
          this.partition(member).answerRecovery(silo, request),
        registeredActivations: async (member, request) =>
          this.partition(member).answerRegistered(request),
      };
      const refresh = async (version: number): Promise<void> => {
        this.refreshed.push({ silo: silo.podName, version });
        if (!this.held.has(silo.podName) && this.latest.version >= version) {
          this.partition(silo).applyView(this.latest);
        }
      };
      const partition = new GrainDirectoryPartition({
        local: silo,
        peers,
        activations: { registeredIn: () => [] },
        refreshView: refresh,
        time: this.time,
      });
      this.partitions.set(silo.podName, partition);
      const peer: DirectoryPeer = {
        lookup: async (owner, version, grainId) => {
          this.peerCalls++;
          return this.partition(owner).lookup(version, grainId);
        },
        register: async (owner, version, a, previous) => {
          this.peerCalls++;
          return this.partition(owner).register(version, a, previous);
        },
        unregister: async (owner, version, a) => {
          this.peerCalls++;
          return this.partition(owner).unregister(version, a);
        },
      };
      this.directories.set(
        silo.podName,
        new DistributedGrainDirectory({ local: silo, partition, peer, refresh, time: this.time }),
      );
    }
  }

  partition(silo: SiloAddress): GrainDirectoryPartition {
    return this.partitions.get(silo.podName)!;
  }

  directory(silo: SiloAddress): DistributedGrainDirectory {
    return this.directories.get(silo.podName)!;
  }

  async publish(v: DirectoryView, to: SiloAddress[], settle = true): Promise<void> {
    this.latest = v;
    for (const silo of to) this.partition(silo).applyView(v);
    await flush();
    if (settle) for (const silo of to) await this.partition(silo).settled();
  }
}

describe("DistributedGrainDirectory", () => {
  it("routes registration and lookup to the owning silo, regardless of caller", async () => {
    const c = new Cluster([S0, S1]);
    await c.publish(
      view(1, [
        [S0, "active"],
        [S1, "active"],
      ]),
      [S0, S1],
    );
    const a = addr(new GrainId("Counter", "k"), S0);
    const winner = await c.directory(S0).register(a);
    expect(winner.activationId).toBe(a.activationId);
    expect(winner.version).toBe(1);
    expect((await c.directory(S1).lookup(a.grainId))?.activationId).toBe(a.activationId);
  });

  it("is compare-and-set across silos: concurrent registrations converge on one winner", async () => {
    const c = new Cluster([S0, S1]);
    await c.publish(
      view(1, [
        [S0, "active"],
        [S1, "active"],
      ]),
      [S0, S1],
    );
    const id = new GrainId("Counter", "race");
    const [w0, w1] = await Promise.all([
      c.directory(S0).register(addr(id, S0)),
      c.directory(S1).register(addr(id, S1)),
    ]);
    expect(w0.activationId).toBe(w1.activationId);
  });

  it("refreshes when the owner says its view is newer, re-resolves the owner and succeeds", async () => {
    const c = new Cluster([S0, S1, S2]);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
    ]);
    await c.publish(v1, [S0, S1, S2]);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
      [S2, "active"],
    ]);
    // A grain S1 owns at v1 and S2 at v2; S0's watch is held, so it still routes to S1.
    const id = grainMoving([
      { view: v1, owner: S1 },
      { view: v2, owner: S2 },
    ]);
    c.held.add(S0.podName);
    // S2 takes its ranges from S0 too, which serves them once it has caught up with v2.
    await c.publish(v2, [S1, S2], false);
    c.held.delete(S0.podName);

    const registering = c.directory(S0).register(addr(id, S0));
    await flush();
    // Told to refresh, it paused before trying again.
    c.time.advance(100);
    const winner = await registering;
    expect(winner.version).toBe(2);
    expect(c.refreshed.some((r) => r.silo === S0.podName && r.version === 2)).toBe(true);
    expect(c.partition(S0).view.version).toBe(2);
    expect(c.partition(S2).entryFor(id)?.activationId).toBe(winner.activationId);
    expect(c.partition(S1).entryFor(id)).toBeUndefined();
  });

  it("pauses between retries and gives up, refused as a stale view, after a bounded number", async () => {
    const c = new Cluster([S0, S1, S2]);
    const v1 = view(1, [
      [S0, "active"],
      [S1, "active"],
    ]);
    await c.publish(v1, [S0, S1, S2]);
    const v2 = view(2, [
      [S0, "active"],
      [S1, "active"],
      [S2, "active"],
    ]);
    const id = grainMoving([
      { view: v1, owner: S1 },
      { view: v2, owner: S2 },
    ]);
    // S0 can never catch up: its refresh delivers nothing.
    c.held.add(S0.podName);
    await c.publish(v2, [S1, S2], false);

    const lookup = c
      .directory(S0)
      .lookup(id)
      .then(
        () => "resolved",
        (err: unknown) => err,
      );
    await flush();
    const callsBeforePause = c.peerCalls;
    await flush();
    // No retry without the clock moving: it is pausing, not spinning.
    expect(c.peerCalls).toBe(callsBeforePause);
    for (let i = 0; i < 20; i++) {
      c.time.advance(1_000);
      await flush();
    }
    const failure = await lookup;
    expect(failure).toBeInstanceOf(RejectionError);
    expect(failure).toMatchObject({ kind: "staleView" });
    expect(c.peerCalls).toBe(10);
  });

  it("sends a request at a view at least as new as its recovery version", async () => {
    const c = new Cluster([S0, S1]);
    await c.publish(
      view(1, [
        [S0, "active"],
        [S1, "active"],
      ]),
      [S0, S1],
    );
    c.latest = view(4, [
      [S0, "active"],
      [S1, "active"],
    ]);
    // A member recovering at v4 asked S0 for its activations.
    c.partition(S0).answerRecovery(S1, { version: 4, ranges: [] });
    expect(c.directory(S0).recoveryVersion).toBe(4);
    await c.directory(S0).lookup(new GrainId("Counter", "x"));
    expect(c.partition(S0).view.version).toBe(4);
  });

  it("rejects an unresolvable owner as a stale view, not a programming fault", async () => {
    const c = new Cluster([S0]);
    await c.publish(view(1, [[S0, "draining"]]), [S0]);
    const failure = await c
      .directory(S0)
      .lookup(new GrainId("Counter", "k"))
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    expect(failure).toBeInstanceOf(RejectionError);
    expect(failure).toMatchObject({ kind: "staleView" });
  });
});
