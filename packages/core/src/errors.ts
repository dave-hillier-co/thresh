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
 * Raised when a per-activation admission limit configured on the turn
 * scheduler is exceeded (Orleans `LimitExceededException`, thrown by
 * `WorkItemGroup` when `SchedulingOptions.MaxEnqueuedRequestsHardLimit` is
 * hit) — here, a turn arrives when the activation's queue is already at
 * `TurnSchedulerOptions.maxEnqueuedRequestsHardLimit`. Transient: the caller
 * should back off and retry, ideally once the activation has drained.
 */
export class LimitExceededException extends Error {
  constructor(
    readonly limitName: string,
    readonly currentValue: number,
    readonly maxValue: number,
  ) {
    super(`limit ${limitName} of ${maxValue} exceeded, current value ${currentValue}`);
    this.name = "LimitExceededException";
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
 * Raised when a call's ambient `AbortSignal`/deadline (see
 * `@thresh/runtime/dispatcher`'s `InvokeCallOptions` and `InvocationRequest
 * .deadline`) fires before the call's turn was ever admitted to run. Orleans
 * has no analogue — this is JS-only cooperative cancellation (see
 * `docs/deviations.md`). Once a turn has actually started it always runs to
 * completion (no thread interruption to preempt it with); this error only
 * preempts a still-queued turn (`TurnScheduler`) or abandons a wait on a
 * storage-provider call (`@thresh/core/abort`'s `raceSignal`).
 */
export class GrainCallAbortedError extends Error {
  constructor(message = "the call was aborted before it completed") {
    super(message);
    this.name = "GrainCallAbortedError";
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
    options?: { cause?: unknown },
  ) {
    super(`transaction ${transactionId} aborted: ${reason}`, options);
    this.name = "TransactionAbortedError";
  }
}

/**
 * Raised when a read-only transaction attempts to write to a grain's
 * transactional state (Orleans `OrleansReadOnlyViolatedException`, a subtype
 * of `OrleansTransactionAbortedException`). Thrown by
 * `TransactionalStateImpl.performUpdate` (`@thresh/transactions`) before any
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
 * `ReaderWriterLock.enter` (`@thresh/transactions`) specifically on the
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

/**
 * Raised when a transaction's root call attempts to resolve while it still has
 * outstanding "orphaned" calls it forked but never awaited to completion
 * (Orleans `OrleansOrphanCallException`, a subtype of
 * `OrleansTransactionAbortedException`). `TransactionInfo.fork()` increments
 * the pending-call count each time application code detaches a call from the
 * transaction's own completion (Orleans `TransactionInfo.Fork`); if that count
 * is still nonzero when the root boundary tries to commit, the transaction
 * cannot be resolved safely and is aborted instead.
 */
export class TransactionOrphanCallError extends TransactionAbortedError {
  constructor(
    transactionId: string,
    readonly pendingCalls: number,
  ) {
    super(
      transactionId,
      `${pendingCalls} orphaned call(s) were still pending when the transaction attempted to resolve`,
    );
    this.name = "TransactionOrphanCallError";
  }
}

/**
 * Raised when a `@readOnly` call attempts to mutate a grain's persistent
 * state — replacing `value` wholesale, mutating a property reached through
 * it, or calling `write()`/`clear()` — while the silo's dev-mode read-only
 * guard is enabled (opt-in, off by default; see
 * `guardPersistentStateForReadOnly`). Mirrors
 * `TransactionReadOnlyViolatedError`'s intent for transactional state, but
 * for ordinary (non-transactional) grain state, where nothing otherwise
 * rejects the write.
 */
export class ReadOnlyStateViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReadOnlyStateViolationError";
  }
}

/**
 * Raised when a transaction is aborted purely because some other,
 * transitively related transaction it read state from has already aborted
 * (Orleans `OrleansCascadingAbortException`, a subtype of
 * `OrleansTransactionTransientFailureException` — transient, worth retrying).
 * Optimistic reads can observe a tentative value written by a transaction that
 * later aborts; once that's discovered, every reader that saw it must abort
 * too rather than commit on data that never became real.
 */
export class TransactionCascadingAbortError extends TransactionAbortedError {
  constructor(transactionId: string, cause?: unknown) {
    super(transactionId, "a transitively related transaction aborted", { cause });
    this.name = "TransactionCascadingAbortError";
  }
}

/**
 * Raised when a transaction's outcome cannot be determined after the commit
 * decision was already durably recorded (Orleans `OrleansTransactionInDoubtException`).
 * Unlike {@link TransactionAbortedError} and its subtypes, this is **not** an
 * abort: by the time it can happen the elected transaction manager has already
 * recorded the commit (`TransactionAgent.resolve`'s `recordCommit` step), so
 * every participant's write *will* eventually be applied — this error means
 * only that one participant's own commit step failed or threw while applying
 * it, so the caller cannot tell from this call alone whether that participant
 * (or others still in flight) finished applying it yet. Deliberately not a
 * subtype of `TransactionAbortedError`, mirroring upstream's
 * `OrleansTransactionInDoubtException : OrleansTransactionException` (a
 * sibling of `OrleansTransactionAbortedException`, not a child of it).
 */
export class TransactionInDoubtError extends Error {
  constructor(
    readonly transactionId: string,
    options?: { cause?: unknown },
  ) {
    super(
      `transaction ${transactionId} is in doubt: its commit was recorded but a participant failed while applying it`,
      options,
    );
    this.name = "TransactionInDoubtError";
  }
}
