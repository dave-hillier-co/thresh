import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const silos = (n: number) => Array.from({ length: n }, (_, i) => silo(i));
const grainIds = (n: number) =>
  Array.from({ length: n }, (_, i) => new GrainId("Counter", `g-${i}`));

describe("ConsistentHashRing", () => {
  it("throws when asked for an owner on an empty ring", () => {
    expect(() => new ConsistentHashRing([]).ownerOf(new GrainId("Counter", "x"))).toThrow();
  });

  it("is deterministic across rings built from the same silo set", () => {
    const a = new ConsistentHashRing(silos(3));
    const b = new ConsistentHashRing([silo(2), silo(0), silo(1)]); // different order
    for (const id of grainIds(200)) {
      expect(a.ownerOf(id).equals(b.ownerOf(id))).toBe(true);
    }
  });

  it("keys ring position off podName, ignoring podUid (so restarts keep ownership)", () => {
    const ring = new ConsistentHashRing([silo(0), silo(1), silo(2)]);
    const restarted = new ConsistentHashRing([
      new SiloAddress("silo-0", "uid-NEW", "silo-0:11111"),
      silo(1),
      silo(2),
    ]);
    for (const id of grainIds(200)) {
      expect(ring.ownerOf(id).ringKey).toBe(restarted.ownerOf(id).ringKey);
    }
  });

  it("distributes ownership roughly evenly", () => {
    const ring = new ConsistentHashRing(silos(4));
    const counts = new Map<string, number>();
    const total = 4000;
    for (const id of grainIds(total)) {
      const key = ring.ownerOf(id).ringKey;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    expect(counts.size).toBe(4);
    const fair = total / 4;
    for (const c of counts.values()) {
      expect(c).toBeGreaterThan(fair * 0.5);
      expect(c).toBeLessThan(fair * 1.5);
    }
  });

  it("reshuffles only a fraction of keys when a silo joins", () => {
    const before = new ConsistentHashRing(silos(3));
    const after = new ConsistentHashRing(silos(4));
    const ids = grainIds(4000);
    const moved = ids.filter((id) => !before.ownerOf(id).equals(after.ownerOf(id))).length;
    // Ideal is ~1/4 of keys move; allow generous slack for hashing variance.
    expect(moved / ids.length).toBeLessThan(0.4);
    expect(moved).toBeGreaterThan(0);
  });

  it("reshuffles only a fraction of keys when a silo leaves", () => {
    const before = new ConsistentHashRing(silos(4));
    const after = new ConsistentHashRing(silos(3));
    const ids = grainIds(4000);
    const moved = ids.filter((id) => !before.ownerOf(id).equals(after.ownerOf(id))).length;
    expect(moved / ids.length).toBeLessThan(0.45);
  });

  describe("rangesFor (reminder ownership)", () => {
    const inRanges = (hash: number, ranges: ReadonlyArray<readonly [number, number]>) =>
      ranges.some(([b, e]) => (b <= e ? hash >= b && hash < e : hash >= b || hash < e));

    it("assigns every hash to exactly one silo's ranges", () => {
      const all = silos(3);
      const ring = new ConsistentHashRing(all);
      const ranges = new Map(all.map((s) => [s.ringKey, ring.rangesFor(s)]));
      for (const id of grainIds(500)) {
        const hash = id.getUniformHashCode();
        const owners = all.filter((s) => inRanges(hash, ranges.get(s.ringKey)!));
        expect(owners).toHaveLength(1);
      }
    });

    it("gives a lone silo the whole ring", () => {
      const ring = new ConsistentHashRing([silo(0)]);
      const ranges = ring.rangesFor(silo(0));
      for (const id of grainIds(500)) {
        expect(inRanges(id.getUniformHashCode(), ranges)).toBe(true);
      }
    });

    it("is agreed by every silo computing the ring from the same view", () => {
      const all = silos(3);
      const a = new ConsistentHashRing(all);
      const b = new ConsistentHashRing([silo(2), silo(0), silo(1)]); // different order
      for (const s of all) {
        expect(a.rangesFor(s)).toEqual(b.rangesFor(s));
      }
    });
  });
});
