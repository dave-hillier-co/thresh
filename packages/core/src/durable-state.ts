/**
 * The grain-facing durable-journaling facets. Each mutation is
 * journalled to the grain's append-only log and replayed on activation; reads
 * are served from memory. Mutators are async because each appends to the log.
 */

/** A single journalled value cell. Orleans' `IDurableValue<T>`. */
export interface DurableValue<T> {
  /** Current value; `undefined` until the first `set`. */
  readonly value: T | undefined;
  get(): T | undefined;
  /** Journal a set and update memory. */
  set(value: T): Promise<void>;
  /** Journal a clear back to `undefined`. */
  clear(): Promise<void>;
}

/** A journalled scalar-keyed map. Orleans' `IDurableDictionary<K,V>`. */
export interface DurableDictionary<K, V> {
  readonly size: number;
  has(key: K): boolean;
  get(key: K): V | undefined;
  entries(): IterableIterator<[K, V]>;
  keys(): IterableIterator<K>;
  values(): IterableIterator<V>;
  /** Journal a set and update memory. */
  set(key: K, value: V): Promise<void>;
  /** Journal a delete; resolves to whether the key existed. */
  delete(key: K): Promise<boolean>;
  /** Journal a clear of all entries. */
  clear(): Promise<void>;
}

/** A journalled ordered list. Orleans' `IDurableList<T>`. */
export interface DurableList<T> {
  readonly length: number;
  get(index: number): T | undefined;
  toArray(): readonly T[];
  [Symbol.iterator](): IterableIterator<T>;
  /** Whether `value` is present. Orleans' `IDurableList<T>.Contains`. */
  contains(value: T): boolean;
  /** Journal an append and update memory. */
  add(value: T): Promise<void>;
  /** Journal a set-at-index and update memory. */
  set(index: number, value: T): Promise<void>;
  /** Journal an insert-at-index, shifting later items back, and update memory. Orleans' `IDurableList<T>.Insert`. */
  insert(index: number, value: T): Promise<void>;
  /** Journal a remove-at-index and update memory. */
  removeAt(index: number): Promise<void>;
  /**
   * Journal a remove of the first occurrence of `value` and update memory;
   * resolves to whether a matching item was found. Orleans' `IDurableList<T>.Remove`.
   */
  remove(value: T): Promise<boolean>;
  /** Journal a clear of all items. */
  clear(): Promise<void>;
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
  /** Journal an enqueue at the back and update memory. */
  enqueue(value: T): Promise<void>;
  /** Journal a dequeue from the front; resolves to the removed element, or `undefined` when empty. */
  dequeue(): Promise<T | undefined>;
  /**
   * Journal a dequeue from the front and resolve to the removed element; throws
   * when empty. Orleans' `IDurableQueue<T>.Dequeue` (which throws `InvalidOperationException`).
   */
  dequeueOrThrow(): Promise<T>;
  /** Journal a clear of all elements. */
  clear(): Promise<void>;
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
  /** Journal an add; resolves to whether the value was newly added. */
  add(value: T): Promise<boolean>;
  /** Journal a delete; resolves to whether the value was present. */
  delete(value: T): Promise<boolean>;
  /** Journal a clear of all values. */
  clear(): Promise<void>;
}
