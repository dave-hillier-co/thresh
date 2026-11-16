import { describe, expect, it } from "vitest";
import { isHashInRange } from "@thresh/core/hash-ring";
import {
  RING_MAX,
  RingRange,
  intersectRanges,
  rangesContain,
  subtractRanges,
} from "@thresh/directory/ring-range";

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

const point = (random: () => number): number => Math.floor(random() * 0x1_0000_0000);

function randomRange(random: () => number): RingRange {
  const roll = random();
  if (roll < 0.05) return RingRange.full;
  if (roll < 0.1) return RingRange.empty;
  return RingRange.of(point(random), point(random));
}

function probesFor(ranges: RingRange[], random: () => number): number[] {
  const out = [0, 1, RING_MAX, RING_MAX - 1];
  for (const r of ranges) {
    for (const p of [r.start, r.end]) out.push(p, (p + 1) >>> 0, (p - 1) >>> 0);
  }
  for (let i = 0; i < 50; i++) out.push(point(random));
  return out;
}

describe("RingRange", () => {
  it("holds (start, end]: the end, not the start", () => {
    const r = RingRange.of(10, 20);
    expect(r.contains(10)).toBe(false);
    expect(r.contains(11)).toBe(true);
    expect(r.contains(20)).toBe(true);
    expect(r.contains(21)).toBe(false);
  });

  it("wraps past the top of the circle when start > end", () => {
    const r = RingRange.of(RING_MAX - 1, 5);
    expect(r.contains(RING_MAX)).toBe(true);
    expect(r.contains(0)).toBe(true);
    expect(r.contains(5)).toBe(true);
    expect(r.contains(RING_MAX - 1)).toBe(false);
    expect(r.contains(6)).toBe(false);
  });

  it("is empty when start equals end, and the empty and full ranges behave", () => {
    expect(RingRange.of(7, 7).isEmpty).toBe(true);
    expect(RingRange.empty.contains(0)).toBe(false);
    expect(RingRange.full.contains(0)).toBe(true);
    expect(RingRange.full.contains(RING_MAX)).toBe(true);
    expect(RingRange.point(0).contains(0)).toBe(true);
    expect(RingRange.point(0).contains(RING_MAX)).toBe(false);
  });

  it("agrees with brute force on intersects, intersection and difference (seeded)", () => {
    const random = mulberry32(3);
    for (let i = 0; i < 400; i++) {
      const a = randomRange(random);
      const b = randomRange(random);
      const both = a.intersection(b);
      const only = a.difference(b);
      let shared = false;
      for (const p of probesFor([a, b], random)) {
        const inBoth = a.contains(p) && b.contains(p);
        shared ||= inBoth;
        expect(rangesContain(both, p), `${a} ∩ ${b} at ${p}`).toBe(inBoth);
        expect(rangesContain(only, p), `${a} - ${b} at ${p}`).toBe(a.contains(p) && !b.contains(p));
      }
      if (shared) expect(a.intersects(b)).toBe(true);
      expect(a.intersects(b)).toBe(both.length > 0);
    }
  });

  it("does the same over collections of ranges", () => {
    const random = mulberry32(9);
    for (let i = 0; i < 100; i++) {
      const a = Array.from({ length: 1 + Math.floor(random() * 4) }, () => randomRange(random));
      const b = Array.from({ length: 1 + Math.floor(random() * 4) }, () => randomRange(random));
      const both = intersectRanges(a, b);
      const only = subtractRanges(a, b);
      for (const p of probesFor([...a, ...b], random)) {
        expect(rangesContain(both, p)).toBe(rangesContain(a, p) && rangesContain(b, p));
        expect(rangesContain(only, p)).toBe(rangesContain(a, p) && !rangesContain(b, p));
      }
    }
  });

  it("converts to the half-open tuple reminder tables read, holding the same points", () => {
    const random = mulberry32(17);
    for (let i = 0; i < 300; i++) {
      const r = randomRange(random);
      if (r.isEmpty) continue;
      const [begin, end] = r.toHashRange();
      for (const p of probesFor([r], random)) {
        expect(isHashInRange(p, begin, end), `${r} at ${p}`).toBe(r.contains(p));
      }
    }
  });
});
