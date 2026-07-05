/** An error raised while dispatching or executing a grain call. */
export class GrainCallError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "GrainCallError";
  }
}

export type RejectionKind =
  | "siloDraining"
  | "unknownTarget"
  | "overloaded"
  | "deserialization"
  | "noActivation"
  | "noCandidates"
  | "staleView";

/** A runtime-level refusal the caller can inspect to decide whether to retry. */
export class RejectionError extends Error {
  constructor(
    message: string,
    readonly kind: RejectionKind,
  ) {
    super(message);
    this.name = "RejectionError";
  }
}

/** A grain call that did not receive a response within its deadline. */
export class GrainCallTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrainCallTimeoutError";
  }
}

/**
 * Raised when a persistent write/clear loses an optimistic-concurrency check:
 * the stored etag no longer matches the one the grain last read, so another
 * incarnation has written in between.
 */
export class InconsistentStateError extends Error {
  constructor(
    message: string,
    readonly expectedEtag: string | undefined,
    readonly storedEtag: string | undefined,
  ) {
    super(message);
    this.name = "InconsistentStateError";
  }
}

/**
 * Raised when a transaction is aborted by the concurrency-control or commit
 * protocol — for example a younger transaction "dies" under wait-die when it
 * conflicts with an older holder, or a participant fails to prepare.
 * Propagates to
 * the originating call so the caller may retry.
 */
export class TransactionAbortedError extends Error {
  constructor(
    readonly transactionId: string,
    reason: string,
  ) {
    super(`transaction ${transactionId} aborted: ${reason}`);
    this.name = "TransactionAbortedError";
  }
}

/**
 * Raised when a read-only transaction attempts to write to a grain's
 * transactional state (Orleans `OrleansReadOnlyViolatedException`, a subtype
 * of `OrleansTransactionAbortedException`). Thrown by
 * `TransactionalStateImpl.performUpdate` (`@tsva/transactions`) before any
 * lock is acquired, since the write is illegal regardless of contention.
 */
export class TransactionReadOnlyViolatedError extends TransactionAbortedError {
  constructor(transactionId: string) {
    super(transactionId, "attempted to write to a grain in a read-only transaction");
    this.name = "TransactionReadOnlyViolatedError";
  }
}
