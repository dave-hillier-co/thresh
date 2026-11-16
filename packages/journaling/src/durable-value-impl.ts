import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import type { DurableValue } from "@thresh/core/durable-state";

type ValueOp<T> = { t: "set"; v: T } | { t: "clear" };

/**
 * A journalled single value, implemented as a `DurableStateMachine`. Each `set`
 * / `clear` is applied to memory at once and buffered on the manager; the next
 * `writeState` appends the buffered entries to the grain's log, and replay
 * reconstructs the value from the log.
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
    return this.value;
  }

  set(value: T): void {
    this.mutate({ t: "set", v: value });
  }

  clear(): void {
    this.mutate({ t: "clear" });
  }

  /** Buffers the op's log entry for the next `writeState` and applies it to memory now. */
  private mutate(op: ValueOp<T>): void {
    // Serialised first: a payload the codec rejects leaves memory untouched, as in Orleans.
    this.manager.buffer(this.name, op);
    this.apply(op);
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
