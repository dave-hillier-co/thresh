import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import type { DurableQueue } from "@tsva/core/durable-state";

type QueueOp<T> = { t: "enq"; v: T } | { t: "deq" } | { t: "clear" } | { t: "load"; items: T[] };

/**
 * A journalled FIFO queue, implemented as a `DurableStateMachine`. Enqueues
 * append to the back and dequeues remove from the front; both replay in append
 * order, so a fresh activation reproduces the same ordering. Each mutation is
 * appended to the grain's log and then applied to memory.
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

  toArray(): readonly T[] {
    return [...this.items];
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this.items[Symbol.iterator]();
  }

  async enqueue(value: T): Promise<void> {
    // The manager applies the op to memory after a successful append.
    await this.manager.append(this.name, { t: "enq", v: value } satisfies QueueOp<T>);
  }

  async dequeue(): Promise<T | undefined> {
    // An empty dequeue is a no-op: nothing to journal, nothing to return.
    if (this.items.length === 0) return undefined;
    const head = this.items[0];
    await this.manager.append(this.name, { t: "deq" } satisfies QueueOp<T>);
    return head;
  }

  async clear(): Promise<void> {
    await this.manager.append(this.name, { t: "clear" } satisfies QueueOp<T>);
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
