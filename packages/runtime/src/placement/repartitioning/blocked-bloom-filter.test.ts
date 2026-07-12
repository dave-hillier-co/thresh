import { GrainId } from "@tsva/core/grain-id";
import { describe, expect, it } from "vitest";
import { AnchoredGrainsFilter, BlockedBloomFilter } from "./blocked-bloom-filter";

const grain = (key: string) => new GrainId("test", key);

describe("BlockedBloomFilter", () => {
  it("contains an element after it is added", () => {
    const filter = new BlockedBloomFilter(100, 0.01);
    const id = grain("key");

    filter.add(id);

    expect(filter.contains(id)).toBe(true);
  });

  it("does not claim to contain elements that were never added", () => {
    const filter = new BlockedBloomFilter(100, 0.01);
    expect(filter.contains(grain("key"))).toBe(false);
  });

  it("has a bounded false-positive rate for a fresh filter", () => {
    const filter = new BlockedBloomFilter(1000, 0.01);
    for (let i = 0; i < 1000; i++) {
      filter.add(grain(`present-${i}`));
    }

    let falsePositives = 0;
    const trials = 5000;
    for (let i = 0; i < trials; i++) {
      if (filter.contains(grain(`absent-${i}`))) {
        falsePositives++;
      }
    }

    // Loose bound: well under 10x the requested 1% rate.
    expect(falsePositives / trials).toBeLessThan(0.1);
  });

  it("rejects a false-positive rate outside [0.001, 0.01]", () => {
    expect(() => new BlockedBloomFilter(100, 0.0001)).toThrow(RangeError);
    expect(() => new BlockedBloomFilter(100, 0.1)).toThrow(RangeError);
  });

  it("reset() clears all previously-added elements", () => {
    const filter = new BlockedBloomFilter(100, 0.01);
    const id = grain("key");
    filter.add(id);
    filter.reset();
    expect(filter.contains(id)).toBe(false);
  });
});

describe("AnchoredGrainsFilter", () => {
  it("contains an element right after it is added", () => {
    const filter = new AnchoredGrainsFilter(100, 0.01, 2);
    const id = grain("key");
    filter.add(id);
    expect(filter.contains(id)).toBe(true);
  });

  it("retains a grain through generations-1 rotations, then drops it", () => {
    const filter = new AnchoredGrainsFilter(100, 0.01, 2);
    const id = grain("key");
    filter.add(id);

    filter.rotate();
    expect(filter.contains(id)).toBe(true);

    filter.rotate();
    expect(filter.contains(id)).toBe(false);
  });

  it("reset() clears every generation", () => {
    const filter = new AnchoredGrainsFilter(100, 0.01, 3);
    const id = grain("key");
    filter.add(id);
    filter.reset();
    expect(filter.contains(id)).toBe(false);
  });

  it("rejects fewer than 1 generation", () => {
    expect(() => new AnchoredGrainsFilter(100, 0.01, 0)).toThrow(RangeError);
  });
});
