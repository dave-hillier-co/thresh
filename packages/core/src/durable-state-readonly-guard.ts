import { guardValue, readOnlyViolation } from "./readonly-guard-value";
import type { DurableKind } from "./durable-state-metadata";
import type {
  DurableDictionary,
  DurableList,
  DurableQueue,
  DurableSet,
  DurableValue,
} from "./durable-state";

/**
 * Wrap an iterator so every yielded value passes through `guardValue` before
 * reaching the caller — mirrors deep-guarding `.value` on the single-cell
 * facets, extended to the collection facets' iteration surface (`entries`,
 * `keys`, `values`, `Symbol.iterator`, `toArray`).
 */
function guardIterator<T>(iter: IterableIterator<T>, stateName: string): IterableIterator<T> {
  return {
    [Symbol.iterator]() {
      return this;
    },
    next(): IteratorResult<T> {
      const result = iter.next();
      return result.done === true
        ? result
        : { done: false, value: guardValue(result.value, stateName) };
    },
  };
}

/**
 * Wrap a `DurableValue<T>` facet (`@durableState`) so every mutation attempt
 * during the wrapped span raises `ReadOnlyStateViolationError` instead of
 * silently succeeding: `value`/`get()` are returned deep-proxied (read-through,
 * write-rejecting; see `guardValue`), and `set()`/`clear()` throw without
 * reaching the underlying journal.
 *
 * Same mechanism as `guardPersistentStateForReadOnly` (`GAP-READONLY-ENFORCEMENT`),
 * applied to durable-journaling facets.
 */
export function guardDurableValueForReadOnly<T>(
  state: DurableValue<T>,
  stateName: string,
): DurableValue<T> {
  return {
    get value(): T | undefined {
      return guardValue(state.value, stateName);
    },
    get(): T | undefined {
      return guardValue(state.get(), stateName);
    },
    async set(_value: T): Promise<void> {
      throw readOnlyViolation(stateName, "set durable value");
    },
    async clear(): Promise<void> {
      throw readOnlyViolation(stateName, "clear durable value");
    },
  };
}

/** Wrap a `DurableDictionary<K, V>` facet (`@durableDictionary`); see {@link guardDurableValueForReadOnly}. */
export function guardDurableDictionaryForReadOnly<K, V>(
  state: DurableDictionary<K, V>,
  stateName: string,
): DurableDictionary<K, V> {
  return {
    get size(): number {
      return state.size;
    },
    has(key: K): boolean {
      return state.has(key);
    },
    get(key: K): V | undefined {
      return guardValue(state.get(key), stateName);
    },
    entries(): IterableIterator<[K, V]> {
      return guardIterator(state.entries(), stateName);
    },
    keys(): IterableIterator<K> {
      return state.keys();
    },
    values(): IterableIterator<V> {
      return guardIterator(state.values(), stateName);
    },
    async set(_key: K, _value: V): Promise<void> {
      throw readOnlyViolation(stateName, "set durable dictionary entry on");
    },
    async delete(_key: K): Promise<boolean> {
      throw readOnlyViolation(stateName, "delete durable dictionary entry on");
    },
    async clear(): Promise<void> {
      throw readOnlyViolation(stateName, "clear durable dictionary");
    },
  };
}

/** Wrap a `DurableList<T>` facet (`@durableList`); see {@link guardDurableValueForReadOnly}. */
export function guardDurableListForReadOnly<T>(
  state: DurableList<T>,
  stateName: string,
): DurableList<T> {
  return {
    get length(): number {
      return state.length;
    },
    get(index: number): T | undefined {
      return guardValue(state.get(index), stateName);
    },
    toArray(): readonly T[] {
      return state.toArray().map((v) => guardValue(v, stateName));
    },
    contains(value: T): boolean {
      return state.contains(value);
    },
    [Symbol.iterator](): IterableIterator<T> {
      return guardIterator(state[Symbol.iterator](), stateName);
    },
    async add(_value: T): Promise<void> {
      throw readOnlyViolation(stateName, "add to durable list");
    },
    async set(_index: number, _value: T): Promise<void> {
      throw readOnlyViolation(stateName, "set durable list entry on");
    },
    async insert(_index: number, _value: T): Promise<void> {
      throw readOnlyViolation(stateName, "insert into durable list");
    },
    async removeAt(_index: number): Promise<void> {
      throw readOnlyViolation(stateName, "remove from durable list");
    },
    async remove(_value: T): Promise<boolean> {
      throw readOnlyViolation(stateName, "remove from durable list");
    },
    async clear(): Promise<void> {
      throw readOnlyViolation(stateName, "clear durable list");
    },
  };
}

/** Wrap a `DurableQueue<T>` facet (`@durableQueue`); see {@link guardDurableValueForReadOnly}. */
export function guardDurableQueueForReadOnly<T>(
  state: DurableQueue<T>,
  stateName: string,
): DurableQueue<T> {
  return {
    get size(): number {
      return state.size;
    },
    peek(): T | undefined {
      return guardValue(state.peek(), stateName);
    },
    peekOrThrow(): T {
      return guardValue(state.peekOrThrow(), stateName);
    },
    toArray(): readonly T[] {
      return state.toArray().map((v) => guardValue(v, stateName));
    },
    [Symbol.iterator](): IterableIterator<T> {
      return guardIterator(state[Symbol.iterator](), stateName);
    },
    async enqueue(_value: T): Promise<void> {
      throw readOnlyViolation(stateName, "enqueue onto durable queue");
    },
    async dequeue(): Promise<T | undefined> {
      throw readOnlyViolation(stateName, "dequeue from durable queue");
    },
    async dequeueOrThrow(): Promise<T> {
      throw readOnlyViolation(stateName, "dequeue from durable queue");
    },
    async clear(): Promise<void> {
      throw readOnlyViolation(stateName, "clear durable queue");
    },
  };
}

/** Wrap a `DurableSet<T>` facet (`@durableSet`); see {@link guardDurableValueForReadOnly}. */
export function guardDurableSetForReadOnly<T>(
  state: DurableSet<T>,
  stateName: string,
): DurableSet<T> {
  return {
    get size(): number {
      return state.size;
    },
    has(value: T): boolean {
      return state.has(value);
    },
    values(): IterableIterator<T> {
      return guardIterator(state.values(), stateName);
    },
    [Symbol.iterator](): IterableIterator<T> {
      return guardIterator(state[Symbol.iterator](), stateName);
    },
    async add(_value: T): Promise<boolean> {
      throw readOnlyViolation(stateName, "add to durable set");
    },
    async delete(_value: T): Promise<boolean> {
      throw readOnlyViolation(stateName, "delete from durable set");
    },
    async clear(): Promise<void> {
      throw readOnlyViolation(stateName, "clear durable set");
    },
  };
}

/**
 * Dispatch to the guard matching a `DurableStateField.kind`, so the runtime
 * can guard a durable-journaling facet without knowing its concrete kind up
 * front — mirrors how `DurableStateField.kind` picks the concrete
 * `DurableStateMachine` implementation in `bindDurableStates`
 * (`@thresh/journaling/durable-state-activator`).
 */
export function guardDurableFieldForReadOnly(
  kind: DurableKind,
  facet: unknown,
  stateName: string,
): unknown {
  switch (kind) {
    case "value":
      return guardDurableValueForReadOnly(facet as DurableValue<unknown>, stateName);
    case "dictionary":
      return guardDurableDictionaryForReadOnly(
        facet as DurableDictionary<unknown, unknown>,
        stateName,
      );
    case "list":
      return guardDurableListForReadOnly(facet as DurableList<unknown>, stateName);
    case "queue":
      return guardDurableQueueForReadOnly(facet as DurableQueue<unknown>, stateName);
    case "set":
      return guardDurableSetForReadOnly(facet as DurableSet<unknown>, stateName);
  }
}
