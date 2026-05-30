import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import type { DurableSet } from "@tsva/core/durable-state";

type SetOp<T> =
  | { t: "add"; v: T }
  | { t: "del"; v: T }
  | { t: "clear" }
  | { t: "load"; values: T[] };

/**
 * A journalled set of scalar values, implemented as a `DurableStateMachine`.
 * Values must be JSON-stable scalars (string/number/boolean) so they round-trip
 * through the value-codec on replay and compare by value. Redundant adds and
 * deletes of absent values are no-ops and journal nothing. Each mutation is
 * appended to the grain's log and then applied to memory.
 */
export class DurableSetImpl<T> implements DurableSet<T>, DurableStateMachine {
  private readonly set = new Set<T>();

  constructor(
    readonly name: string,
    private readonly manager: StateMachineManager,
  ) {}

  get size(): number {
    return this.set.size;
  }

  has(value: T): boolean {
    return this.set.has(value);
  }

  values(): IterableIterator<T> {
    return this.set.values();
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this.set[Symbol.iterator]();
  }

  async add(value: T): Promise<boolean> {
    if (this.set.has(value)) return false;
    // The manager applies the op to memory after a successful append.
    await this.manager.append(this.name, { t: "add", v: value } satisfies SetOp<T>);
    return true;
  }

  async delete(value: T): Promise<boolean> {
    if (!this.set.has(value)) return false;
    await this.manager.append(this.name, { t: "del", v: value } satisfies SetOp<T>);
    return true;
  }

  async clear(): Promise<void> {
    await this.manager.append(this.name, { t: "clear" } satisfies SetOp<T>);
  }

  reset(): void {
    this.set.clear();
  }

  apply(payload: unknown): void {
    const op = payload as SetOp<T>;
    switch (op.t) {
      case "add":
        this.set.add(op.v);
        break;
      case "del":
        this.set.delete(op.v);
        break;
      case "clear":
        this.set.clear();
        break;
      case "load":
        this.set.clear();
        for (const v of op.values) this.set.add(v);
        break;
    }
  }

  snapshot(): unknown {
    return { t: "load", values: [...this.set] } satisfies SetOp<T>;
  }
}
