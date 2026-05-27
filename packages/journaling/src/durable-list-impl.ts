import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import type { DurableList } from "@tsva/core/durable-state";

type ListOp<T> =
  | { t: "add"; v: T }
  | { t: "set"; i: number; v: T }
  | { t: "removeAt"; i: number }
  | { t: "clear" }
  | { t: "load"; items: T[] };

/**
 * A journalled ordered list, implemented as a `DurableStateMachine`. Index-based
 * ops journal the index at append time; replay reproduces the same indices
 * because entries are applied in append order. Each mutation is appended to the
 * grain's log and then applied to memory.
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

  [Symbol.iterator](): IterableIterator<T> {
    return this.items[Symbol.iterator]();
  }

  async add(value: T): Promise<void> {
    // The manager applies the op to memory after a successful append.
    await this.manager.append(this.name, { t: "add", v: value } satisfies ListOp<T>);
  }

  async set(index: number, value: T): Promise<void> {
    await this.manager.append(this.name, { t: "set", i: index, v: value } satisfies ListOp<T>);
  }

  async removeAt(index: number): Promise<void> {
    await this.manager.append(this.name, { t: "removeAt", i: index } satisfies ListOp<T>);
  }

  async clear(): Promise<void> {
    await this.manager.append(this.name, { t: "clear" } satisfies ListOp<T>);
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
