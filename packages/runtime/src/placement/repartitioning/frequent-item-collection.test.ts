import { describe, expect, it } from "vitest";
import { FrequentItemCollection } from "./frequent-item-collection";

class UlongCollection extends FrequentItemCollection<bigint> {
  protected override getKey(element: bigint): bigint {
    return element;
  }
  remove(key: bigint): void {
    this.removeCore(key);
  }
  clear(): void {
    this.clearCore();
  }
}

describe("FrequentItemCollection", () => {
  it("tracks up to capacity distinct keys", () => {
    const sink = new UlongCollection(3);
    sink.add(1n);
    sink.add(2n);
    sink.add(3n);

    expect(sink.count).toBe(3);
    expect(sink.elements.map((e) => e.element).sort()).toEqual([1n, 2n, 3n]);
  });

  it("recovers heavy hitters from a skewed stream", () => {
    const sink = new UlongCollection(5);
    // Key 0 is overwhelmingly the most frequent; keys 1..99 are noise.
    for (let i = 0; i < 5000; i++) {
      sink.add(0n);
    }
    for (let i = 0; i < 1000; i++) {
      sink.add(BigInt(1 + (i % 99)));
    }

    const top = sink.elements.slice().sort((a, b) => b.count - a.count)[0]!;
    expect(top.element).toBe(0n);
    expect(top.count).toBeGreaterThan(1000);
  });

  it("remove() drops a tracked key", () => {
    const sink = new UlongCollection(10);
    sink.add(5n);
    sink.remove(5n);
    expect(sink.elements).toHaveLength(0);
  });

  it("clear() empties the collection", () => {
    const sink = new UlongCollection(10);
    sink.add(1n);
    sink.add(2n);
    sink.clear();
    expect(sink.count).toBe(0);
  });
});
