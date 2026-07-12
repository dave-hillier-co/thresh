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
 * Raised when a call targets a `GrainExtension` interface that has not been
 * bound (via `GrainRuntime.getOrSetExtension`) on the receiving activation,
 * and no auto-install factory is configured for it either (Orleans
 * `GrainExtensionNotInstalledException`). Distinct from `GrainCallError`'s
 * generic "no method" case: the interface is a recognised extension, it's
 * just not installed on this particular activation.
 */
export class GrainExtensionNotInstalledException extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GrainExtensionNotInstalledException";
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
 * Raised when a `[Transaction]`-style grain call (a method with a
 * `TransactionOption` other than `"suppress"`/`"notAllowed"`/`undefined`) is
 * invoked on a silo that was built without transaction support (Orleans
 * `OrleansTransactionsDisabledException`). Transactions are opt-in — a silo
 * must configure transactional storage (e.g. `useMemoryTransactionalStorage`)
 * for `[Transaction]` calls to work.
 */
export class TransactionsDisabledError extends Error {
  constructor() {
    super(
      "Orleans transactions have not been enabled. Transactions are disabled by default and must be configured to be used.",
    );
    this.name = "TransactionsDisabledError";
  }
}

/**
 * Raised when a gateway silo is currently overloaded (Orleans
 * `GatewayTooBusyException`): its `OverloadDetector` reports the silo's CPU
 * usage above `LoadSheddingOptions.cpuThreshold`, so it refuses to accept a
 * new client-originated request rather than queue behind an already
 * struggling process. Transient — the same or another gateway is likely to
 * accept the message if retransmitted later. Thrown client-side by
 * `ClientNode.invoke` when a gateway's reply carries the `"overloaded"`
 * rejection kind.
 */
export class GatewayTooBusyException extends Error {
  constructor(message = "Gateway too busy") {
    super(message);
    this.name = "GatewayTooBusyException";
  }
}

/**
 * Raised when a cooperatively-cancelled grain call observes its
 * `GrainCancellationToken` aborted and stops itself (Orleans
 * `TaskCanceledException`). JS has no thread interruption, so this is thrown
 * by application code (typically via `GrainCancellationToken
 * .throwIfCancellationRequested()` or a cancellable await) rather than by the
 * runtime tearing down the call.
 */
export class GrainTaskCanceledError extends Error {
  constructor(message = "the operation was cancelled via a GrainCancellationToken") {
    super(message);
    this.name = "GrainTaskCanceledError";
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

/**
 * Raised when a transaction cannot upgrade a shared (read) lock it already
 * holds to a write lock because a higher-priority (older) transaction is
 * concurrently holding or waiting on the same resource (Orleans
 * `OrleansTransactionLockUpgradeException`, a subtype of
 * `OrleansTransactionTransientFailureException`). Thrown by
 * `ReaderWriterLock.enter` (`@tsva/transactions`) specifically on the
 * read-to-write upgrade path — never on an ordinary first-acquisition
 * wait-die death, which keeps raising the generic
 * {@link TransactionAbortedError}.
 */
export class TransactionLockUpgradeError extends TransactionAbortedError {
  constructor(transactionId: string) {
    super(
      transactionId,
      "could not upgrade a lock, because of a higher-priority conflicting transaction",
    );
    this.name = "TransactionLockUpgradeError";
  }
}
