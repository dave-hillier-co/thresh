/**
 * "Filtered Space-Saving" top-K heavy-hitter sketch, ported from Orleans'
 * `FrequentItemCollection<TKey, TElement>`
 * (`src/Orleans.Runtime/Placement/Repartitioning/FrequentItemCollection.cs`).
 * See Homem & Carvalho, "Finding top-k elements in data streams"
 * (https://www.hlt.inesc-id.pt/~fmmb/references/misnis.ref0a.pdf), itself a
 * modification of the "Space-Saving" algorithm (Metwally, Agrawal, Abbadi).
 *
 * Bounded to `capacity` tracked elements: once full, a new element is only
 * admitted if a probabilistic count-min-style sketch estimates it occurs at
 * least as often as the current minimum-count tracked element, which is then
 * evicted (its count estimate seeded into the sketch so it can win its slot
 * back later).
 *
 * Deliberate simplification: upstream hand-rolls an amortized-growth array
 * for the heap (matching .NET's `PriorityQueue` internals) to avoid GC
 * pressure; this port uses a plain JS array with push/truncate, which is
 * observably equivalent (same heap-index invariants, same eviction/removal
 * behavior) but relies on the engine's array growth instead of a manual
 * doubling strategy.
 */

const ARITY = 4;
const LOG2_ARITY = 2;
const SKETCH_ENTRIES_PER_HEAP_ENTRY = 6;

interface Counter<TElement> {
  element: TElement;
  count: number;
  error: number;
}

function compareCounters<TElement>(a: Counter<TElement>, b: Counter<TElement>): number {
  const av = (BigInt(a.count) << 32n) | BigInt(0xffffffff - a.error);
  const bv = (BigInt(b.count) << 32n) | BigInt(0xffffffff - b.error);
  return av < bv ? -1 : av > bv ? 1 : 0;
}

/** Mirrors `ulong.GetHashCode()`: XOR of the high and low 32-bit words. */
function bigintHashCode(key: bigint): number {
  const lo = Number(BigInt.asUintN(32, key));
  const hi = Number(BigInt.asUintN(32, key >> 32n));
  return (lo ^ hi) | 0;
}

function getSketchSize(capacity: number): number {
  // Suggested constants in the paper, chap 6, equation (24). Round to nearest power of 2 for
  // cheap binning without modulo.
  const entries = capacity * SKETCH_ENTRIES_PER_HEAP_ENTRY;
  const bits = 32 - Math.clz32(entries);
  return 1 << bits;
}

function getParentIndex(index: number): number {
  return (index - 1) >> LOG2_ARITY;
}

function getFirstChildIndex(index: number): number {
  return (index << LOG2_ARITY) + 1;
}

/**
 * Tracks up to `capacity` `TElement`s (keyed by a `bigint` derived from each element), and their
 * approximate frequency, in unspecified order.
 */
export abstract class FrequentItemCollection<TElement> {
  private heap: Counter<TElement>[] = [];
  private readonly heapIndex = new Map<bigint, number>();
  private readonly sketch: Uint32Array;

  constructor(readonly capacity: number) {
    this.sketch = new Uint32Array(getSketchSize(capacity));
  }

  get count(): number {
    return this.heap.length;
  }

  /** Up to `capacity` elements, along with their count estimates, in unspecified order. */
  get elements(): ReadonlyArray<{ element: TElement; count: number; error: number }> {
    return this.heap.map((c) => ({ element: c.element, count: c.count, error: c.error }));
  }

  protected abstract getKey(element: TElement): bigint;

  add(element: TElement): void {
    const increment = 1;
    const key = this.getKey(element);

    // Increase the count of a key that is already being tracked. There is a minute chance of a
    // hash collision, which is deemed acceptable and ignored.
    const existingIndex = this.heapIndex.get(key);
    if (existingIndex !== undefined) {
      const counter = this.heap[existingIndex]!;
      counter.count += increment;
      this.moveUpHeap(counter, existingIndex, key);
      return;
    }

    // Key is not being tracked, but can fit in the top K, so add it.
    if (this.count < this.capacity) {
      this.insertHeap({ element, count: increment, error: 0 }, key);
      return;
    }

    const min = this.heap[0]!;

    // Filter out values which are estimated to have appeared less frequently than the minimum.
    const sketchMask = this.sketch.length - 1;
    const sketchHash = bigintHashCode(key);
    const countEstimate = this.sketch[sketchHash & sketchMask]!;
    if (countEstimate + increment < min.count) {
      this.sketch[sketchHash & sketchMask] = countEstimate + increment;
      return;
    }

    // Remove the minimum element from the hash index.
    const minKey = this.getKey(min.element);
    this.heapIndex.delete(minKey);

    // While evicting the minimum element, update its counter in the sketch to improve the chance
    // of it passing the filter in the future.
    const minHash = bigintHashCode(minKey);
    this.sketch[minHash & sketchMask] = min.count;

    // Push the new element in place of the last and move it down until it's in position.
    this.moveDownHeap({ element, count: countEstimate + increment, error: countEstimate }, 0, key);
  }

  /** Removes the counter corresponding to `key`. Returns `true` if a matching entry was found. */
  protected removeCore(key: bigint): boolean {
    const sketchMask = this.sketch.length - 1;
    const sketchHash = bigintHashCode(key);
    this.sketch[sketchHash & sketchMask] = 0;

    const index = this.heapIndex.get(key);
    if (index === undefined) {
      return false;
    }
    this.heapIndex.delete(key);

    const newSize = this.heap.length - 1;
    const lastNode = this.heap[newSize]!;
    this.heap.length = newSize; // truncate first, so moveDownHeap sees the new (smaller) size

    if (index < newSize) {
      // Removing an element from the middle of the heap: sift the popped-off last element
      // downward from the removed index.
      this.moveDownHeap(lastNode, index, this.getKey(lastNode.element));
    }

    return true;
  }

  protected clearCore(): void {
    this.heap = [];
    this.heapIndex.clear();
    this.sketch.fill(0);
  }

  private insertHeap(counter: Counter<TElement>, key: bigint): void {
    const index = this.heap.length;
    this.heap.push(counter);
    this.moveUpHeap(counter, index, key);
  }

  private moveUpHeap(node: Counter<TElement>, nodeIndex: number, nodeKey: bigint): void {
    const nodes = this.heap;
    const index = this.heapIndex;

    while (nodeIndex > 0) {
      const parentIndex = getParentIndex(nodeIndex);
      const parent = nodes[parentIndex]!;

      if (compareCounters(node, parent) < 0) {
        nodes[nodeIndex] = parent;
        index.set(this.getKey(parent.element), nodeIndex);
        nodeIndex = parentIndex;
      } else {
        break;
      }
    }

    nodes[nodeIndex] = node;
    index.set(nodeKey, nodeIndex);
  }

  private moveDownHeap(node: Counter<TElement>, nodeIndex: number, nodeKey: bigint): void {
    const nodes = this.heap;
    const size = this.heap.length;
    const index = this.heapIndex;

    let i: number;
    while ((i = getFirstChildIndex(nodeIndex)) < size) {
      let minChild = nodes[i]!;
      let minChildIndex = i;

      const childIndexUpperBound = Math.min(i + ARITY, size);
      for (++i; i < childIndexUpperBound; i++) {
        const nextChild = nodes[i]!;
        if (compareCounters(nextChild, minChild) < 0) {
          minChild = nextChild;
          minChildIndex = i;
        }
      }

      if (compareCounters(node, minChild) <= 0) {
        break;
      }

      nodes[nodeIndex] = minChild;
      index.set(this.getKey(minChild.element), nodeIndex);
      nodeIndex = minChildIndex;
    }

    index.set(nodeKey, nodeIndex);
    nodes[nodeIndex] = node;
  }
}
