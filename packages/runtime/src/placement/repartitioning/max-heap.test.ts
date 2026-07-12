import { describe, expect, it } from "vitest";
import { type HeapElement, MaxHeap } from "./max-heap";

class NumberElement implements HeapElement<NumberElement> {
  heapIndex = -1;
  constructor(public value: number) {}
  compareTo(other: NumberElement): number {
    return this.value - other.value;
  }
}

describe("MaxHeap", () => {
  it("pops elements in descending order regardless of insertion order", () => {
    const values = [5, 3, 8, 1, 9, 2, 7, 4, 6, 0];
    const elements = values.map((v) => new NumberElement(v));
    const heap = new MaxHeap(elements);

    expect(heap.count).toBe(10);

    const popped: number[] = [];
    while (heap.count > 0) {
      popped.push(heap.pop().value);
    }

    expect(popped).toEqual([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
  });

  it("peek() returns the max without removing it", () => {
    const heap = new MaxHeap([new NumberElement(1), new NumberElement(2)]);
    expect(heap.peek().value).toBe(2);
    expect(heap.count).toBe(2);
  });

  it("throws when popping/peeking an empty heap", () => {
    const heap = new MaxHeap<NumberElement>([]);
    expect(() => heap.pop()).toThrow();
    expect(() => heap.peek()).toThrow();
  });

  it("onIncreaseElementPriority reflects a raised value at the root", () => {
    const elements = [new NumberElement(1), new NumberElement(2), new NumberElement(3)];
    const heap = new MaxHeap(elements);

    const smallest = elements[0]!;
    smallest.value = 100;
    heap.onIncreaseElementPriority(smallest);

    expect(heap.peek().value).toBe(100);
  });

  it("onDecreaseElementPriority sinks a lowered value away from the root", () => {
    const elements = [new NumberElement(1), new NumberElement(2), new NumberElement(3)];
    const heap = new MaxHeap(elements);

    const largest = elements.find((e) => e.value === 3)!;
    largest.value = -100;
    heap.onDecreaseElementPriority(largest);

    expect(heap.peek().value).toBe(2);
  });
});
