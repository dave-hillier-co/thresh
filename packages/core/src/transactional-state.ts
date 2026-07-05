/**
 * The grain-facing transactional-state facet, mirroring Orleans
 * `ITransactionalState<T>`.
 * State is reached only inside a transaction, through `performRead` (a read that
 * must not mutate) and `performUpdate` (a mutation applied to a per-transaction
 * tentative copy). The facet enlists itself as a participant on first access; the
 * runtime commits or aborts it at the transaction boundary. Serializable
 * isolation is enforced by a timestamp-ordered, wait-die lock on the resource.
 */
/** Options for a single {@link TransactionalState.performRead} call. */
export interface PerformReadOptions {
  /**
   * Take a write (exclusive) lock for this read instead of a shared read
   * lock, mirroring Orleans' `[UseExclusiveLock]`. A read-then-write
   * sequence on the same grain that marks its read exclusive serializes with
   * other transactions up front, instead of racing a later read-to-write
   * lock upgrade.
   */
  exclusive?: boolean;
}

export interface TransactionalState<T> {
  /** Read the state visible to the current transaction. The reader must not mutate it. */
  performRead<R>(read: (state: T) => R, options?: PerformReadOptions): Promise<R>;
  /** Mutate the current transaction's tentative copy of the state. */
  performUpdate<R>(update: (state: T) => R): Promise<R>;
}
