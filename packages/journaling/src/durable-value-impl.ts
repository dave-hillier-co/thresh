import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import type { DurableValue } from "@tsva/core/durable-state";

type ValueOp<T> = { t: "set"; v: T } | { t: "clear" };

/**
 * A journalled single value, implemented as a `DurableStateMachine`. Each `set`
 * / `clear` is appended to the grain's log through the manager and then applied
 * to memory; replay reconstructs the value from the log.
 */
export class DurableValueImpl<T> implements DurableValue<T>, DurableStateMachine {
  private current: T | undefined;
  private present = false;

  constructor(
    readonly name: string,
    private readonly manager: StateMachineManager,
  ) {}

  get value(): T | undefined {
    return this.current;
  }

  get(): T | undefined {
    return this.current;
  }

  async set(value: T): Promise<void> {
    // The manager applies the op to memory after a successful append.
    await this.manager.append(this.name, { t: "set", v: value } satisfies ValueOp<T>);
  }

  async clear(): Promise<void> {
    await this.manager.append(this.name, { t: "clear" } satisfies ValueOp<T>);
  }

  reset(): void {
    this.current = undefined;
    this.present = false;
  }

  apply(payload: unknown): void {
    const op = payload as ValueOp<T>;
    if (op.t === "set") {
      this.current = op.v;
      this.present = true;
    } else {
      this.current = undefined;
      this.present = false;
    }
  }

  snapshot(): unknown {
    return this.present
      ? ({ t: "set", v: this.current } as ValueOp<T>)
      : ({ t: "clear" } as ValueOp<T>);
  }
}
