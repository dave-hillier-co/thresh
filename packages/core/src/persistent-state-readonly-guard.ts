import { ReadOnlyStateViolationError } from "./errors";
import type { PersistentState } from "./persistent-state";

/**
 * Deep-wrap a value reached through a guarded `PersistentState.value` in a
 * `Proxy` that rejects any write reaching it — property assignment, delete,
 * `defineProperty`, `setPrototypeOf` — with `ReadOnlyStateViolationError`.
 * Nested objects/arrays returned by `get` are wrapped lazily, on the way out,
 * so `state.value.orders.push(x)` is caught (array mutators like `push` read
 * `this.length`/write `this[i]` internally, which the proxy's own traps see)
 * without having to walk the whole object graph up front.
 */
function guardValue<T>(value: T, stateName: string): T {
  if (value === null || typeof value !== "object") return value;
  const violation = (): never => {
    throw new ReadOnlyStateViolationError(
      `attempted to mutate persistent state "${stateName}" during a read-only call`,
    );
  };
  return new Proxy(value as object, {
    get(target, prop, receiver) {
      return guardValue(Reflect.get(target, prop, receiver), stateName);
    },
    set: violation,
    deleteProperty: violation,
    defineProperty: violation,
    setPrototypeOf: violation,
  }) as T;
}

/**
 * Wrap a `PersistentState<T>` facet so every mutation attempt during the
 * wrapped span raises `ReadOnlyStateViolationError` instead of silently
 * succeeding: `value` is returned deep-proxied (read-through, write-rejecting;
 * see `guardValue`), reassigning `value` wholesale throws, and `write()` /
 * `clear()` throw without reaching the underlying storage. `read()` passes
 * through unchanged — reloading from storage does not mutate anything the
 * grain owns.
 *
 * This is the mechanism behind the silo's opt-in, dev-mode `@readOnly`
 * mutation guard (`GAP-READONLY-ENFORCEMENT`): the runtime swaps a grain's
 * `@persistentState` fields for guarded wrappers only for the duration of a
 * `readOnly` turn, restoring the real facet once the turn ends, so a normal
 * (non-`readOnly`) call never pays for it.
 */
export function guardPersistentStateForReadOnly<T>(
  state: PersistentState<T>,
  stateName: string,
): PersistentState<T> {
  return {
    get value(): T {
      return guardValue(state.value, stateName);
    },
    set value(_next: T) {
      throw new ReadOnlyStateViolationError(
        `attempted to replace persistent state "${stateName}" during a read-only call`,
      );
    },
    get etag(): string | undefined {
      return state.etag;
    },
    get exists(): boolean {
      return state.exists;
    },
    read(): Promise<void> {
      return state.read();
    },
    async write(): Promise<void> {
      throw new ReadOnlyStateViolationError(
        `attempted to write persistent state "${stateName}" during a read-only call`,
      );
    },
    async clear(): Promise<void> {
      throw new ReadOnlyStateViolationError(
        `attempted to clear persistent state "${stateName}" during a read-only call`,
      );
    },
  };
}
