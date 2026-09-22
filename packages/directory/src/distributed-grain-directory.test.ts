import { describe, expect, it } from "vitest";
import { newActivationId } from "@thresh/core/activation-id";
import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import type { DirectoryPeer } from "@thresh/directory/directory-peer";
import { DistributedGrainDirectory } from "@thresh/directory/distributed-grain-directory";
import { LocalDirectoryPartition } from "@thresh/directory/local-directory-partition";

const siloA = new SiloAddress("silo-A", "ua", "a:1");
const siloB = new SiloAddress("silo-B", "ub", "b:1");
const siloC = new SiloAddress("silo-C", "uc", "c:1");
const silos = [siloA, siloB];
const ring = new ConsistentHashRing(silos);
const addr = (key: string, silo: SiloAddress): GrainAddress => ({
  grainId: new GrainId("Counter", key),
  silo,
  activationId: newActivationId(),
});

/** Routes peer operations to the owning silo's partition, modelling the RPC. */
function buildCluster() {
  const partitions = new Map<string, LocalDirectoryPartition>([
    [siloA.ringKey, new LocalDirectoryPartition()],
    [siloB.ringKey, new LocalDirectoryPartition()],
  ]);
  const peer: DirectoryPeer = {
    lookup: async (owner, grainId) => partitions.get(owner.ringKey)!.lookup(grainId),
    register: async (owner, a, previous) => partitions.get(owner.ringKey)!.register(a, previous),
    unregister: async (owner, a) => partitions.get(owner.ringKey)!.unregister(a),
  };
  const make = (self: SiloAddress) =>
    new DistributedGrainDirectory(self, partitions.get(self.ringKey)!, () => ring, peer);
  return { dirA: make(siloA), dirB: make(siloB), partitions };
}

describe("DistributedGrainDirectory", () => {
  it("routes registration and lookup to the owning silo, regardless of caller", () => {
    return (async () => {
      const { dirA, dirB } = buildCluster();
      const a = addr("k", siloA);
      const winner = await dirA.register(a);
      expect(winner.activationId).toBe(a.activationId);
      // Looked up from the other silo, resolves to the same owner partition.
      const fromB = await dirB.lookup(a.grainId);
      expect(fromB?.activationId).toBe(a.activationId);
    })();
  });

  it("is compare-and-set across silos: concurrent registrations converge on one winner", async () => {
    const { dirA, dirB } = buildCluster();
    const grainKey = "race";
    const fromA = addr(grainKey, siloA);
    const fromB = addr(grainKey, siloB);
    const winnerA = await dirA.register(fromA);
    const winnerB = await dirB.register(fromB);
    // Whoever the owning partition accepted first wins for both callers.
    expect(winnerA.activationId).toBe(winnerB.activationId);
    expect(winnerA.activationId).toBe(fromA.activationId);
  });

  it("refreshes its view and re-resolves the owner when a peer reports a stale view", async () => {
    const partition = new LocalDirectoryPartition();
    const a = addr("k", siloA);
    partition.register(a);

    // Our (stale) ring routes the grain to siloB; the peer rejects as stale.
    // refresh() corrects the ring to point at the local owner, where it resolves.
    let owner: SiloAddress = siloB;
    const peer: DirectoryPeer = {
      lookup: async () => {
        throw new RejectionError("stale directory view", "staleView");
      },
      register: async () => a,
      unregister: async () => undefined,
    };
    const stub = { ownerOf: () => owner } as unknown as ConsistentHashRing;

    let refreshed = 0;
    const dir = new DistributedGrainDirectory(
      siloA,
      partition,
      () => stub,
      peer,
      () => {
        refreshed++;
        owner = siloA;
      },
    );

    expect(await dir.lookup(a.grainId)).toEqual(a);
    expect(refreshed).toBe(1);
  });

  it("re-resolves instead of writing into a partition that lost the range while it awaited", async () => {
    // The owned-here path picks the owner, awaits `onOwnedAccess` — which yields a
    // microtask even when there is no recovery to wait for — and only then calls
    // `onOwned`. `updateView` runs on exactly such a microtask, so the ring can
    // move this range elsewhere inside that yield: writing regardless leaves an
    // entry in a partition the ring no longer assigns, nothing re-drains it until
    // the next view change, and the grain is then reactivated as a second
    // activation with divergent state.
    const partitions = new Map<string, LocalDirectoryPartition>([
      [siloA.ringKey, new LocalDirectoryPartition()],
      [siloB.ringKey, new LocalDirectoryPartition()],
      [siloC.ringKey, new LocalDirectoryPartition()],
    ]);
    const peer: DirectoryPeer = {
      lookup: async (owner, grainId) => partitions.get(owner.ringKey)!.lookup(grainId),
      register: async (owner, a, previous) => partitions.get(owner.ringKey)!.register(a, previous),
      unregister: async (owner, a) => partitions.get(owner.ringKey)!.unregister(a),
    };
    // A key silo-A owns at two silos and the joining silo-C takes over at three.
    let current = new ConsistentHashRing(silos);
    const key = keyOwnedBy([
      { ring: current, owner: siloA },
      { ring: new ConsistentHashRing([...silos, siloC]), owner: siloC },
    ]);
    const dir = new DistributedGrainDirectory(
      siloA,
      partitions.get(siloA.ringKey)!,
      () => current,
      peer,
      () => undefined,
      async () => {
        // The view change, landing in the owned-access yield: the range is
        // silo-C's from here on.
        await Promise.resolve();
        current = new ConsistentHashRing([...silos, siloC]);
      },
    );

    const a = addr(key, siloA);
    await dir.register(a);

    expect(partitions.get(siloA.ringKey)!.lookup(a.grainId)).toBeUndefined();
    expect(partitions.get(siloC.ringKey)!.lookup(a.grainId)).toEqual(a);
  });

  it("drops local entries pointing at a departed silo", async () => {
    const { dirA, partitions } = buildCluster();
    // Force entries owned by A that point at A and at B.
    const ownedByA = grainKeysOwnedBy(siloA, 5);
    for (const key of ownedByA) await dirA.register(addr(key, siloB));
    expect(partitions.get(siloA.ringKey)!.size).toBe(ownedByA.length);
    await dirA.unregisterSilo(siloB);
    expect(partitions.get(siloA.ringKey)!.size).toBe(0);
  });
});

/** First `Counter/*` key every `(ring, owner)` pair agrees on — arranges a deterministic move. */
function keyOwnedBy(
  moves: ReadonlyArray<{ ring: ConsistentHashRing; owner: SiloAddress }>,
): string {
  for (let i = 0; ; i++) {
    const id = new GrainId("Counter", `moved-${i}`);
    if (moves.every(({ ring, owner }) => ring.ownerOf(id).equals(owner))) return `moved-${i}`;
  }
}

function grainKeysOwnedBy(silo: SiloAddress, count: number): string[] {
  const keys: string[] = [];
  for (let i = 0; keys.length < count; i++) {
    const id = new GrainId("Counter", `owned-${i}`);
    if (ring.ownerOf(id).equals(silo)) keys.push(`owned-${i}`);
  }
  return keys;
}
