import { guardValue, readOnlyViolation } from "./readonly-guard-value";
import type { ReducerState } from "./reducer-state";

/**
 * Wrap a `ReducerState<TState, TEvent>` facet so every mutation attempt during
 * the wrapped span raises `ReadOnlyStateViolationError` instead of silently
 * succeeding: `value` is returned deep-proxied (read-through, write-rejecting;
 * see `guardValue`) so in-place mutation of the folded state is caught even
 * though `ReducerState` has no settable `value`, `raise()` throws before
 * folding the event (so the underlying `value` never advances), and `write()`
 * throws without reaching the underlying storage. `read()` passes through
 * unchanged — reloading the snapshot from storage does not mutate anything
 * the grain owns.
 *
 * Same mechanism as `guardPersistentStateForReadOnly`, applied to
 * `@reducerState` fields (`GAP-READONLY-ENFORCEMENT`): the runtime swaps a
 * grain's reducer facets for guarded wrappers only for the duration of a
 * `readOnly` turn, restoring the real facet once the turn ends.
 */
export function guardReducerStateForReadOnly<TState, TEvent>(
  state: ReducerState<TState, TEvent>,
  stateName: string,
): ReducerState<TState, TEvent> {
  return {
    get value(): TState {
      return guardValue(state.value, stateName);
    },
    get etag(): string | undefined {
      return state.etag;
    },
    get exists(): boolean {
      return state.exists;
    },
    raise(_event: TEvent): TState {
      throw readOnlyViolation(stateName, "raise an event onto reducer");
    },
    read(): Promise<void> {
      return state.read();
    },
    async write(): Promise<void> {
      throw readOnlyViolation(stateName, "write reducer");
    },
  };
}
