/**
 * The grain-facing durable-journaling facets, mirroring Orleans.Journaling. A mutator is a
 * synchronous in-memory operation that buffers its log entry; nothing reaches storage until the
 * grain writes its durable state (`writeDurableState()` on the class base, the function
 * `useWriteDurableState()` returns in a hook grain), which appends everything buffered across all
 * of the grain's structures in ONE storage write. Reads are served from memory and see a mutation
 * at once. On activation the log is replayed, so only written mutations survive.
 */

/** A single journalled value cell. Orleans' `IDurableValue<T>`. */
export interface DurableValue<T> {
  /** Current value; `undefined` until the first `set`. */
  readonly value: T | undefined;
  get(): T | undefined;
  /** Set in memory and buffer the log entry. */
  set(value: T): void;
  /** Clear back to `undefined` in memory and buffer the log entry. */
  clear(): void;
}

/** A journalled scalar-keyed map. Orleans' `IDurableDictionary<K,V>`. */
export interface DurableDictionary<K, V> {
  readonly size: number;
  has(key: K): boolean;
  get(key: K): V | undefined;
  entries(): IterableIterator<[K, V]>;
  keys(): IterableIterator<K>;
  values(): IterableIterator<V>;
  /** Set in memory and buffer the log entry. */
  set(key: K, value: V): void;
  /** Delete in memory and buffer the log entry; returns whether the key existed. */
  delete(key: K): boolean;
  /** Clear all entries in memory and buffer the log entry. */
  clear(): void;
}

/** A journalled ordered list. Orleans' `IDurableList<T>`. */
export interface DurableList<T> {
  readonly length: number;
  get(index: number): T | undefined;
  toArray(): readonly T[];
  [Symbol.iterator](): IterableIterator<T>;
  /** Whether `value` is present. Orleans' `IDurableList<T>.Contains`. */
  contains(value: T): boolean;
  /** Append in memory and buffer the log entry. */
  add(value: T): void;
  /** Set at an index in memory and buffer the log entry. */
  set(index: number, value: T): void;
  /** Insert at an index, shifting later items back, in memory and buffer the log entry. Orleans' `IDurableList<T>.Insert`. */
  insert(index: number, value: T): void;
  /** Remove at an index in memory and buffer the log entry. */
  removeAt(index: number): void;
  /**
   * Remove the first occurrence of `value` in memory and buffer the log entry;
   * returns whether a matching item was found. Orleans' `IDurableList<T>.Remove`.
   */
  remove(value: T): boolean;
  /** Clear all items in memory and buffer the log entry. */
  clear(): void;
}

/** A journalled FIFO queue. Orleans' `IDurableQueue<T>`. */
export interface DurableQueue<T> {
  readonly size: number;
  /** The element at the front without removing it; `undefined` when empty. */
  peek(): T | undefined;
  /**
   * The element at the front without removing it; throws when empty.
   * Orleans' `IDurableQueue<T>.Peek` (which throws `InvalidOperationException`).
   */
  peekOrThrow(): T;
  toArray(): readonly T[];
  [Symbol.iterator](): IterableIterator<T>;
  /** Enqueue at the back in memory and buffer the log entry. */
  enqueue(value: T): void;
  /** Dequeue from the front in memory and buffer the log entry; returns the removed element, or `undefined` when empty. */
  dequeue(): T | undefined;
  /**
   * Dequeue from the front and return the removed element; throws
   * when empty. Orleans' `IDurableQueue<T>.Dequeue` (which throws `InvalidOperationException`).
   */
  dequeueOrThrow(): T;
  /** Clear all elements in memory and buffer the log entry. */
  clear(): void;
}

/**
 * A journalled set of scalar values. Orleans' `IDurableSet<T>`. Values must be
 * JSON-stable scalars (string/number/boolean) so they round-trip through the
 * value-codec on replay and compare by value rather than reference.
 */
export interface DurableSet<T> {
  readonly size: number;
  has(value: T): boolean;
  values(): IterableIterator<T>;
  [Symbol.iterator](): IterableIterator<T>;
  /** Add in memory and buffer the log entry; returns whether the value was newly added. */
  add(value: T): boolean;
  /** Delete in memory and buffer the log entry; returns whether the value was present. */
  delete(value: T): boolean;
  /** Clear all values in memory and buffer the log entry. */
  clear(): void;
}
