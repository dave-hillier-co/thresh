import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import type { DurableList } from "@thresh/core/durable-state";

type ListOp<T> =
  | { t: "add"; v: T }
  | { t: "set"; i: number; v: T }
  | { t: "removeAt"; i: number }
  | { t: "insert"; i: number; v: T }
  | { t: "clear" }
  | { t: "load"; items: T[] };

/**
 * A journalled ordered list, implemented as a `DurableStateMachine`. Index-based
 * ops journal the index at append time; replay reproduces the same indices
 * because entries are applied in append order. Each mutation is applied to
 * memory at once and buffered; `writeState` appends the buffer.
 */
export class DurableListImpl<T> implements DurableList<T>, DurableStateMachine {
  private items: T[] = [];

  constructor(
    readonly name: string,
    private readonly manager: StateMachineManager,
  ) {}

  get length(): number {
    return this.items.length;
  }

  get(index: number): T | undefined {
    return this.items[index];
  }

  toArray(): readonly T[] {
    return [...this.items];
  }

  /** Whether `value` is present, compared by `Array.prototype.includes` semantics. */
  contains(value: T): boolean {
    return this.items.includes(value);
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this.items[Symbol.iterator]();
  }

  add(value: T): void {
    this.mutate({ t: "add", v: value });
  }

  set(index: number, value: T): void {
    this.mutate({ t: "set", i: index, v: value });
  }

  removeAt(index: number): void {
    this.mutate({ t: "removeAt", i: index });
  }

  /** Insert at an index, shifting later items back. */
  insert(index: number, value: T): void {
    this.mutate({ t: "insert", i: index, v: value });
  }

  /** Remove the first occurrence of `value`; returns whether a matching item was found. */
  remove(value: T): boolean {
    const index = this.items.indexOf(value);
    if (index === -1) return false;
    this.removeAt(index);
    return true;
  }

  clear(): void {
    this.mutate({ t: "clear" });
  }

  /** Buffers the op's log entry for the next `writeState` and applies it to memory now. */
  private mutate(op: ListOp<T>): void {
    // Serialised first: a payload the codec rejects leaves memory untouched, as in Orleans.
    this.manager.buffer(this.name, op);
    this.apply(op);
  }

  reset(): void {
    this.items = [];
  }

  apply(payload: unknown): void {
    const op = payload as ListOp<T>;
    switch (op.t) {
      case "add":
        this.items.push(op.v);
        break;
      case "set":
        this.items[op.i] = op.v;
        break;
      case "removeAt":
        this.items.splice(op.i, 1);
        break;
      case "insert":
        this.items.splice(op.i, 0, op.v);
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
    return { t: "load", items: [...this.items] } satisfies ListOp<T>;
  }
}
