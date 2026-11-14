import { readOnlyViolation } from "./readonly-guard-value";
import type { PerformReadOptions, TransactionalState } from "./transactional-state";

/**
 * Wrap a `TransactionalState<T>` facet (`@transactionalState`) so
 * `performUpdate` during the wrapped span raises `ReadOnlyStateViolationError`
 * before touching the lock or enlisting as a write participant, instead of
 * silently succeeding. `performRead` passes through unchanged — the facet's
 * own contract already requires the reader not to mutate the state it
 * receives (see `TransactionalState.performRead`'s doc comment).
 *
 * This closes a real gap distinct from `TransactionReadOnlyViolatedError`
 * (`TransactionalStateImpl.performUpdate`, `@thresh/transactions`): that error
 * fires only when the whole *transaction* was started read-only
 * (`TransactionInfo.readOnly`, set from `startTransaction`'s caller). A
 * `@readOnly` *grain call* can join an ambient transaction that a different,
 * non-`readOnly` caller started — `TransactionInfo.readOnly` is fixed for
 * the transaction's lifetime and shared by every participant, so the
 * transaction-level check does not see the individual call's `readOnly`
 * flag and lets that call's `performUpdate` through. This guard, wired the
 * same way as `guardPersistentStateForReadOnly`
 * (`GAP-READONLY-ENFORCEMENT`), closes that gap by keying off the call's own
 * `readOnly` flag instead.
 */
export function guardTransactionalStateForReadOnly<T>(
  state: TransactionalState<T>,
  stateName: string,
): TransactionalState<T> {
  const guarded: TransactionalState<T> = {
    performRead<R>(read: (state: T) => R, options?: PerformReadOptions): Promise<R> {
      return state.performRead(read, options);
    },
    async performUpdate<R>(_update: (state: T) => R): Promise<R> {
      throw readOnlyViolation(stateName, "write transactional");
    },
  };
  guardedOriginals.set(guarded, state);
  return guarded;
}

/** Each guard back to the facet it wraps. */
const guardedOriginals = new WeakMap<object, unknown>();

/**
 * The facet behind a field value that may be a read-only guard. The
 * transaction protocol calls (`prepare`, `commit`, …) are always-interleave, so
 * one can arrive while a `@readOnly` turn has the field swapped for its guard;
 * they must still reach the real resource.
 */
export function unguardedTransactionalState(value: unknown): unknown {
  return typeof value === "object" && value !== null
    ? (guardedOriginals.get(value) ?? value)
    : value;
}
