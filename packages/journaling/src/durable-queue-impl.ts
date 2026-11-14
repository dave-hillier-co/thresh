import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import type { DurableQueue } from "@thresh/core/durable-state";

type QueueOp<T> = { t: "enq"; v: T } | { t: "deq" } | { t: "clear" } | { t: "load"; items: T[] };

/**
 * A journalled FIFO queue, implemented as a `DurableStateMachine`. Enqueues
 * append to the back and dequeues remove from the front; both replay in append
 * order, so a fresh activation reproduces the same ordering. Each mutation is
 * applied to memory at once and buffered; `writeState` appends the buffer.
 */
export class DurableQueueImpl<T> implements DurableQueue<T>, DurableStateMachine {
  private items: T[] = [];

  constructor(
    readonly name: string,
    private readonly manager: StateMachineManager,
  ) {}

  get size(): number {
    return this.items.length;
  }

  peek(): T | undefined {
    return this.items[0];
  }

  /**
   * The element at the front without removing it. Orleans' `IDurableQueue<T>.Peek()`,
   * which throws `InvalidOperationException` on an empty queue rather than
   * returning a sentinel; `peek()` above stays the non-throwing option.
   */
  peekOrThrow(): T {
    if (this.items.length === 0) {
      throw new Error("Queue is empty.");
    }
    return this.items[0] as T;
  }

  toArray(): readonly T[] {
    return [...this.items];
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this.items[Symbol.iterator]();
  }

  enqueue(value: T): void {
    this.mutate({ t: "enq", v: value });
  }

  dequeue(): T | undefined {
    // An empty dequeue is a no-op: nothing to journal, nothing to return.
    if (this.items.length === 0) return undefined;
    const head = this.items[0];
    this.mutate({ t: "deq" });
    return head;
  }

  /** Buffers the op's log entry for the next `writeState` and applies it to memory now. */
  private mutate(op: QueueOp<T>): void {
    // Serialised first: a payload the codec rejects leaves memory untouched, as in Orleans.
    this.manager.buffer(this.name, op);
    this.apply(op);
  }

  /**
   * Removes and returns the front element. Orleans' `IDurableQueue<T>.Dequeue()`,
   * which throws `InvalidOperationException` on an empty queue; `dequeue()`
   * above stays the non-throwing (no-op) option.
   */
  dequeueOrThrow(): T {
    if (this.items.length === 0) {
      throw new Error("Queue is empty.");
    }
    return this.dequeue() as T;
  }

  clear(): void {
    this.mutate({ t: "clear" });
  }

  reset(): void {
    this.items = [];
  }

  apply(payload: unknown): void {
    const op = payload as QueueOp<T>;
    switch (op.t) {
      case "enq":
        this.items.push(op.v);
        break;
      case "deq":
        this.items.shift();
        break;
      case "clear":
        this.items = [];
        break;
      case "load":
        this.items = [...op.items];
        break;
    }
  }

  snapshot(): unknown {
    return { t: "load", items: [...this.items] } satisfies QueueOp<T>;
  }
}
