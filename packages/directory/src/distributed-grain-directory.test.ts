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

function grainKeysOwnedBy(silo: SiloAddress, count: number): string[] {
  const keys: string[] = [];
  for (let i = 0; keys.length < count; i++) {
    const id = new GrainId("Counter", `owned-${i}`);
    if (ring.ownerOf(id).equals(silo)) keys.push(`owned-${i}`);
  }
  return keys;
}
