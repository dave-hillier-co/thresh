/**
 * Array-backed quaternary max-heap, ported from Orleans'
 * `MaxHeap<TElement>` (`src/Orleans.Runtime/Placement/Repartitioning/FrequentItemCollection.cs`).
 * Elements track their own heap index so priority changes can be applied
 * in place via `onIncreaseElementPriority`/`onDecreaseElementPriority`
 * rather than requiring a full re-heapify.
 */

const ARITY = 4;
const LOG2_ARITY = 2;

export interface HeapElement<TElement> {
  heapIndex: number;
  compareTo(other: TElement): number;
}

function getParentIndex(index: number): number {
  return (index - 1) >> LOG2_ARITY;
}

function getFirstChildIndex(index: number): number {
  return (index << LOG2_ARITY) + 1;
}

export class MaxHeap<TElement extends HeapElement<TElement>> {
  private nodes: TElement[];
  private size: number;

  constructor(items: readonly TElement[]) {
    this.size = items.length;
    this.nodes = items.slice();

    for (let i = 0; i < this.nodes.length; i++) {
      this.nodes[i]!.heapIndex = i;
    }

    if (this.size > 1) {
      this.heapify();
    } else if (this.size === 1) {
      this.nodes[0]!.heapIndex = 0;
    }
  }

  get count(): number {
    return this.size;
  }

  firstOrDefault(): TElement | undefined {
    return this.size > 0 ? this.nodes[0] : undefined;
  }

  tryPeek(): { found: true; value: TElement } | { found: false; value: undefined } {
    if (this.size > 0) {
      return { found: true, value: this.nodes[0]! };
    }
    return { found: false, value: undefined };
  }

  peek(): TElement {
    if (this.size === 0) {
      throw new Error("Collection is empty.");
    }
    return this.nodes[0]!;
  }

  tryPop(): { found: true; value: TElement } | { found: false; value: undefined } {
    if (this.size > 0) {
      return { found: true, value: this.pop() };
    }
    return { found: false, value: undefined };
  }

  pop(): TElement {
    if (this.size === 0) {
      throw new Error("Collection is empty.");
    }

    const element = this.nodes[0]!;
    const lastNodeIndex = --this.size;

    if (lastNodeIndex > 0) {
      const lastNode = this.nodes[lastNodeIndex]!;
      this.moveDown(lastNode, 0);
    }

    element.heapIndex = -1;
    return element;
  }

  onDecreaseElementPriority(element: TElement): void {
    // If the element has already been removed from the heap, this is a no-op.
    if (element.heapIndex < 0) {
      return;
    }
    this.moveDown(element, element.heapIndex);
  }

  onIncreaseElementPriority(element: TElement): void {
    // If the element has already been removed from the heap, this is a no-op.
    if (element.heapIndex <= 0) {
      return;
    }
    this.moveUp(element, element.heapIndex);
  }

  /** Converts an unordered list into a heap. */
  private heapify(): void {
    const lastParentWithChildren = getParentIndex(this.size - 1);
    for (let index = lastParentWithChildren; index >= 0; --index) {
      this.moveDown(this.nodes[index]!, index);
    }
  }

  /** The elements in this collection, without any ordering guarantees. */
  get unorderedElements(): readonly TElement[] {
    return this.nodes.slice(0, this.size);
  }

  private moveUp(node: TElement, nodeIndex: number): void {
    const nodes = this.nodes;

    while (nodeIndex > 0) {
      const parentIndex = getParentIndex(nodeIndex);
      const parentNode = nodes[parentIndex]!;

      if (node.compareTo(parentNode) <= 0) {
        break;
      }

      nodes[nodeIndex] = parentNode;
      parentNode.heapIndex = nodeIndex;
      nodeIndex = parentIndex;
    }

    nodes[nodeIndex] = node;
    node.heapIndex = nodeIndex;
  }

  private moveDown(node: TElement, nodeIndex: number): void {
    const nodes = this.nodes;
    const size = this.size;

    let i: number;
    while ((i = getFirstChildIndex(nodeIndex)) < size) {
      let maxChild = nodes[i]!;
      let maxChildIndex = i;

      const childIndexUpperBound = Math.min(i + ARITY, size);
      for (++i; i < childIndexUpperBound; i++) {
        const nextChild = nodes[i]!;
        if (nextChild.compareTo(maxChild) > 0) {
          maxChild = nextChild;
          maxChildIndex = i;
        }
      }

      if (node.compareTo(maxChild) >= 0) {
        break;
      }

      nodes[nodeIndex] = maxChild;
      maxChild.heapIndex = nodeIndex;
      nodeIndex = maxChildIndex;
    }

    nodes[nodeIndex] = node;
    node.heapIndex = nodeIndex;
  }
}
