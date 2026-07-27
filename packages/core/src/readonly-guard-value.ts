import { ReadOnlyStateViolationError } from "./errors";

/**
 * Deep-wrap a value reached through a guarded state facet in a `Proxy` that
 * rejects any write reaching it — property assignment, delete,
 * `defineProperty`, `setPrototypeOf` — with `ReadOnlyStateViolationError`.
 * Nested objects/arrays returned by `get` are wrapped lazily, on the way out,
 * so e.g. `state.value.orders.push(x)` is caught (array mutators like `push`
 * read `this.length`/write `this[i]` internally, which the proxy's own traps
 * see) without having to walk the whole object graph up front.
 *
 * Shared by every `@readOnly` state-facet guard (persistent, reducer,
 * durable, transactional — `GAP-READONLY-ENFORCEMENT`) so they all reject
 * in-place mutation of a value reached through a read accessor the same way.
 */
export function guardValue<T>(value: T, stateName: string): T {
  if (value === null || typeof value !== "object") return value;
  const violation = (): never => {
    throw new ReadOnlyStateViolationError(
      `attempted to mutate state "${stateName}" during a read-only call`,
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

/** Build a `ReadOnlyStateViolationError` for a rejected mutation attempt, naming the state and the attempted operation. */
export function readOnlyViolation(
  stateName: string,
  attempted: string,
): ReadOnlyStateViolationError {
  return new ReadOnlyStateViolationError(
    `attempted to ${attempted} state "${stateName}" during a read-only call`,
  );
}
