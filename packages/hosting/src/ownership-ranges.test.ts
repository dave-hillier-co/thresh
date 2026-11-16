import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { isHashInRanges } from "@thresh/core/hash-ring";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { ownedQueueIndices, queueRingHash } from "@thresh/streams/queue-ownership";

/**
 * One arc definition serves the directory, reminders and streams: a virtual node owns
 * `(previous vnode, this vnode]`, so the silo `ownerOf` names for a hash is exactly the silo whose
 * reminder and stream ranges contain it. A property over random rings, driven by a seeded
 * sequence so a failure replays.
 */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randomRing(random: () => number): { silos: SiloAddress[]; ring: ConsistentHashRing } {
  const count = 1 + Math.floor(random() * 6);
  const silos = Array.from(
    { length: count },
    (_, i) => new SiloAddress(`pod-${Math.floor(random() * 1e9)}-${i}`, `uid-${i}`, `ep-${i}:1`),
  );
  return { silos, ring: new ConsistentHashRing(silos) };
}

/** Hashes worth probing: every vnode boundary and its neighbours, the ends of the circle, and noise. */
function probes(ring: ConsistentHashRing, silos: SiloAddress[], random: () => number): number[] {
  const points = new Set<number>([0, 1, 0xffff_ffff, 0xffff_fffe]);
  for (const silo of silos) {
    for (const arc of ring.arcsOf(silo)) {
      for (const p of [arc.start, arc.end]) {
        points.add(p >>> 0);
        points.add((p + 1) >>> 0);
        points.add((p - 1) >>> 0);
      }
    }
  }
  for (let i = 0; i < 200; i++) points.add(Math.floor(random() * 0x1_0000_0000));
  return [...points];
}

describe("ownership ranges (one arc definition)", () => {
  for (const seed of [1, 7, 42, 1234, 99_991]) {
    it(`tiles the circle exactly as directory ownership does (seed ${seed})`, () => {
      const random = mulberry32(seed);
      for (let round = 0; round < 3; round++) {
        const { silos, ring } = randomRing(random);
        const ranges = new Map(silos.map((s) => [s.ringKey, ring.rangesFor(s)]));
        const arcs = new Map(silos.map((s) => [s.ringKey, ring.arcsOf(s)]));
        for (const hash of probes(ring, silos, random)) {
          const holders = silos.filter((s) => isHashInRanges(hash, ranges.get(s.ringKey)!));
          expect(holders, `hash ${hash}`).toHaveLength(1);
          expect(holders[0]!.equals(ring.owner(hash)), `hash ${hash}`).toBe(true);
          const arcHolders = silos.filter((s) =>
            arcs.get(s.ringKey)!.some((a) => a.contains(hash)),
          );
          expect(arcHolders, `hash ${hash}`).toHaveLength(1);
          expect(arcHolders[0]!.equals(ring.owner(hash))).toBe(true);
        }
      }
    });
  }

  it("assigns each stream queue to the silo the directory says owns its point", () => {
    const random = mulberry32(5);
    for (let round = 0; round < 20; round++) {
      const { silos, ring } = randomRing(random);
      const queueCount = 64;
      const seen = new Map<number, SiloAddress>();
      for (const silo of silos) {
        for (const q of ownedQueueIndices("prov", queueCount, ring.rangesFor(silo))) {
          expect(seen.has(q), `queue ${q} owned twice`).toBe(false);
          seen.set(q, silo);
        }
      }
      expect(seen.size).toBe(queueCount);
      for (const [q, silo] of seen) {
        expect(silo.equals(ring.owner(queueRingHash("prov", q)))).toBe(true);
      }
    }
  });

  it("reads each reminder from the table once, at the silo that owns its grain", async () => {
    const random = mulberry32(11);
    const table = new MemoryReminderTable();
    const grains = Array.from({ length: 300 }, (_, i) => new GrainId("R", `g-${i}`));
    for (const grainId of grains) {
      await table.upsert({
        grainId,
        name: "r",
        startAt: new Date(0),
        period: { seconds: 1 },
      });
    }
    for (let round = 0; round < 10; round++) {
      const { silos, ring } = randomRing(random);
      const readBy = new Map<string, SiloAddress[]>();
      for (const silo of silos) {
        for (const [begin, end] of ring.rangesFor(silo)) {
          for (const entry of await table.readRange(begin, end)) {
            const key = entry.grainId.toString();
            readBy.set(key, [...(readBy.get(key) ?? []), silo]);
          }
        }
      }
      for (const grainId of grains) {
        const readers = readBy.get(grainId.toString()) ?? [];
        expect(readers, grainId.toString()).toHaveLength(1);
        expect(readers[0]!.equals(ring.ownerOf(grainId))).toBe(true);
      }
    }
  });
});
