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
  | "noCandidates";

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
 * conflicts with an older holder, or a participant fails to prepare
 * ([ADR 0008](../../docs/adr/0008-cross-grain-transactions.md)). Propagates to
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
