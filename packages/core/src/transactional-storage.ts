import type { GrainId } from "./grain-id";
import type { ParticipantId } from "./transaction-info";

/**
 * Durable storage for transactional state, ported from Orleans
 * `ITransactionalStateStorage<TState>`.
 * Distinct from `GrainStorage`: it keeps a committed version with a monotonic
 * local sequence id, a list of prepared-but-uncommitted pending states, and a
 * commit-records log, so a commit is a single atomic `store` and recovery can
 * resolve in-doubt transactions.
 */

/** A prepared, not-yet-committed transaction's state, kept until commit or abort. */
export interface PendingTransactionState<T> {
  /**
   * Local sequence number, unique to the record for as long as it is staged
   * (minting may skip values, so the ids need not be dense); a re-prepare with
   * the same id replaces it.
   */
  sequenceId: number;
  transactionId: string;
  /** Logical commit timestamp. */
  timeStamp: number;
  /** The TM that knows this transaction's fate; recovery asks it whether to commit. */
  transactionManager?: ParticipantId | undefined;
  /** Snapshot of the state after this transaction executed. */
  state: T;
}

/**
 * The TM's durable record that a transaction committed (Orleans `CommitRecord`).
 * Written in the same store as the TM's own write; removed once every other
 * write participant has confirmed. A TM that re-activates with records still
 * present resumes confirming them.
 */
export interface CommitRecord {
  timeStamp: number;
  /** Every write participant, the TM included; the ones still to confirm. */
  writeParticipants: ParticipantId[];
}

/** Algorithm metadata stored alongside the state (the TM's commit log). */
export interface TransactionalStateMetadata {
  timeStamp: number;
  commitRecords: Record<string, CommitRecord>;
}

export interface TransactionalStorageLoadResponse<T> {
  etag: string | undefined;
  committedState: T;
  /** Local sequence id of the last committed transaction, or 0 if none. */
  committedSequenceId: number;
  metadata: TransactionalStateMetadata;
  /** Prepared pending states, ordered by sequence id. */
  pendingStates: PendingTransactionState<T>[];
}

/**
 * `signal`, when given, is an ambient cancellation/deadline signal for a call
 * (see `GrainStorage`'s doc — same contract). A provider honours it on a
 * best-effort basis against its own network calls; a provider that cannot
 * cancel mid-flight may instead only abandon the *wait* for an already-sent
 * call (`@thresh/core/abort`'s `raceSignal`), or ignore it entirely.
 */
export interface TransactionalStateStorage<T = unknown> {
  load(
    stateName: string,
    grainId: GrainId,
    signal?: AbortSignal,
  ): Promise<TransactionalStorageLoadResponse<T>>;

  /**
   * Atomically: prepare the given pending states; if `commitUpTo` is set, commit
   * all pending up to and including that sequence id; if `abortAfter` is set,
   * drop all pending with a strictly larger sequence id. Returns the new etag;
   * throws on an etag mismatch.
   *
   * Both thresholds are sound because the pending states form a chain: each
   * transaction's tentative state is built on the newest pending one, so a
   * record commits only after every record below it has (a confirmed record
   * subsumes the ones below it), and aborting one aborts every record above
   * it, which was built on it (Orleans `TransactionQueue`).
   */
  store(
    stateName: string,
    grainId: GrainId,
    expectedETag: string | undefined,
    metadata: TransactionalStateMetadata,
    statesToPrepare: ReadonlyArray<PendingTransactionState<T>>,
    commitUpTo: number | undefined,
    abortAfter: number | undefined,
    signal?: AbortSignal,
  ): Promise<string>;
}
