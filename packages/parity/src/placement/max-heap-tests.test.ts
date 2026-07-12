// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/MaxHeapTests.cs @ v10.1.0 (MIT).
//
// `HeapPropertyIsMaintained` inserts random elements into the repartitioner's
// internal `MaxHeap<T>`/`IHeapElement<T>` and asserts extraction always
// returns them in descending order, including after priorities are mutated
// and re-heapified via `OnIncreaseElementPriority`/`OnDecreaseElementPriority`.
// Ported onto `@tsva/runtime/placement/repartitioning/max-heap`
// (`MaxHeap`/`HeapElement`), a faithful port of the quaternary array-backed
// max-heap.
import { type HeapElement, MaxHeap } from "@tsva/runtime/placement/repartitioning/max-heap";
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

class MyHeapElement implements HeapElement<MyHeapElement> {
  heapIndex = -1;
  constructor(public value: number) {}
  compareTo(other: MyHeapElement): number {
    return this.value - other.value;
  }
}

/** Fisher–Yates shuffle, mirroring `Random.Shared.Shuffle`. */
function shuffle<T>(array: T[]): void {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j]!, array[i]!];
  }
}

describe("UnitTests.ActivationRepartitioningTests.MaxHeapTests", () => {
  orleansTest("UnitTests.ActivationRepartitioningTests.MaxHeapTests.HeapPropertyIsMaintained", () => {
    const edges: MyHeapElement[] = [];
    for (let i = 0; i < 100; i++) {
      edges.push(new MyHeapElement(i));
    }

    shuffle(edges);
    const heap = new MaxHeap(edges);
    expect(heap.count).toBe(100);
    expect(heap.peek().value).toBe(99);
    expect(heap.peek().value).toBe(99);
    expect(heap.pop().value).toBe(99);
    expect(heap.pop().value).toBe(98);
    expect(heap.count).toBe(98);
    expect(heap.unorderedElements.length).toBe(98);

    const unorderedElements = heap.unorderedElements.slice();
    const edge = unorderedElements[Math.floor(Math.random() * unorderedElements.length)]!;
    edge.value = 2000;
    heap.onIncreaseElementPriority(edge);
    expect(heap.peek().value).toBe(2000);

    // Randomly re-assign priorities to edges.
    let newScore = 100;
    const elements = heap.unorderedElements.slice();
    shuffle(elements);
    for (const element of elements) {
      const originalValue = element.value;
      element.value = newScore--;
      if (element.value > originalValue) {
        heap.onIncreaseElementPriority(element);
      } else {
        heap.onDecreaseElementPriority(element);
      }
    }

    expect(heap.unorderedElements.length).toBe(98);
    const allElements: MyHeapElement[] = [];
    while (heap.count > 0) {
      allElements.push(heap.pop());
    }

    expect(allElements).toHaveLength(98);

    const expected = Array.from({ length: 98 }, (_, i) => 100 - i).join(", ");
    const actual = allElements.map((c) => c.value).join(", ");
    expect(actual).toBe(expected);
  });
});
