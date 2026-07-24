import { ReadOnlyStateViolationError } from "./errors";
import { guardValue } from "./readonly-guard-value";
import type { PersistentState } from "./persistent-state";

/**
 * Wrap a `PersistentState<T>` facet so every mutation attempt during the
 * wrapped span raises `ReadOnlyStateViolationError` instead of silently
 * succeeding: `value` is returned deep-proxied (read-through, write-rejecting;
 * see `guardValue` in `./readonly-guard-value`), reassigning `value` wholesale
 * throws, and `write()` / `clear()` throw without reaching the underlying
 * storage. `read()` passes through unchanged — reloading from storage does
 * not mutate anything the grain owns.
 *
 * This is the mechanism behind the silo's opt-in, dev-mode `@readOnly`
 * mutation guard (`GAP-READONLY-ENFORCEMENT`): the runtime swaps a grain's
 * `@persistentState` fields for guarded wrappers only for the duration of a
 * `readOnly` turn, restoring the real facet once the turn ends, so a normal
 * (non-`readOnly`) call never pays for it. `./reducer-state-readonly-guard`,
 * `./durable-state-readonly-guard` and `./transactional-state-readonly-guard`
 * apply the identical pattern to the other state facets a grain can declare.
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
