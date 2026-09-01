/**
 * The catch-all base beneath the grain-call failure family — Thresh's answer to
 * Orleans' `OrleansException`, which is the base of `SiloUnavailableException`,
 * `OrleansMessageRejectionException` and friends. A transliterated
 * `catch (OrleansException)` becomes `if (isThreshRuntimeError(error))` rather
 * than an open-coded `instanceof` list that every newly-added error type
 * silently falls through as "unexpected".
 *
 * Deliberately a NEW class ABOVE the existing leaves rather than a promotion of
 * one of them: `RejectionError` must not become an `instanceof GrainCallError`,
 * or every existing narrowing over the leaves quietly widens.
 *
 * Cancellation is NOT part of this family — see {@link ThreshCancellationError}.
 * Orleans' `OperationCanceledException` is likewise not an `OrleansException`,
 * and a consumer classifies cancellation before transport: conflating the two
 * turns a caller's cancellation into a retriable availability failure.
 */
export class ThreshRuntimeError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ThreshRuntimeError";
  }
}

/**
 * Whether `error` is any Thresh grain-call failure — the one predicate that
 * stands in for a C# `catch (OrleansException)`.
 *
 * Narrow deliberately: a `TypeError`, a `RangeError` or a plain `Error` is a
 * programming fault and must NOT be reported as a retriable transport failure,
 * so this never widens to a bare `Error`.
 */
export function isThreshRuntimeError(error: unknown): error is ThreshRuntimeError {
  return error instanceof ThreshRuntimeError;
}

const RESERVED_FALLBACK_KEYS = new Set(["name", "message", "stack", "errorType", "properties"]);

/**
 * The stand-in for an error whose concrete class the receiving process cannot rebuild — Orleans'
 * `UnavailableExceptionFallbackException`, which its `ExceptionCodec` produces when the wire's
 * type name does not resolve locally. Carries the original {@link errorType} (also mirrored onto
 * `name`, which is how JavaScript discriminates errors), the message, and the original's own
 * enumerable properties — both under {@link properties} and copied onto the instance, so a
 * transliterated `error.limit` still reads.
 *
 * Deliberately a plain `Error` and NOT a {@link ThreshRuntimeError}: upstream's fallback derives
 * from `Exception`, not `OrleansException`, and a domain error that classified as a Thresh
 * transport failure would be retried when the failure is permanent. Registering a surrogate for
 * the type (see `docs/orleans-to-thresh-port.md`) is still what makes it arrive as ITSELF; this is
 * the floor beneath that, not a replacement for it.
 */
export class UnavailableExceptionFallbackException extends Error {
  /** The `name` the error carried on the sending side — the type this process could not rebuild. */
  readonly errorType: string;
  /** The sender's own enumerable properties, decoded. Also copied onto this instance. */
  readonly properties: Readonly<Record<string, unknown>>;
  /**
   * The decoded member errors of an `AggregateError` this process could not rebuild as itself (an
   * `AggregateError` subclass with its own `name`, e.g.). `errors` is a non-enumerable own property
   * on the real class, so it is installed explicitly here — from the dedicated `errors` argument —
   * rather than relying solely on the properties-copy loop below.
   *
   * `errors` is deliberately NOT in {@link RESERVED_FALLBACK_KEYS}: `value-codec.ts`'s decoder
   * merges a dedicated wire `errors` array into `properties.errors` too (same reference), so the
   * loop below re-applying it is a harmless no-op — and reserving it would have silently dropped an
   * OLD encoder's payload (pre-issue-#63) that had no dedicated `errors`/`cause` fields at all and
   * instead carried them as ordinary enumerable properties inside `properties`. Letting the loop
   * reach them is what makes that older wire shape still land something useful.
   */
  readonly errors?: readonly unknown[];

  constructor(
    errorType: string,
    message: string,
    properties: Record<string, unknown> = {},
    errors?: readonly unknown[],
  ) {
    super(message);
    this.errorType = errorType;
    // `name` carries the sending type, not this class's own: a caller discriminates on `name` in
    // JavaScript the way a C# caller discriminates on the exception type, and `instanceof
    // UnavailableExceptionFallbackException` is already the answer to "was the type rebuilt?".
    this.name = errorType;
    this.properties = properties;
    if (errors !== undefined) this.errors = errors;
    for (const [key, value] of Object.entries(properties)) {
      // Never let a carried property overwrite the identity fields above. `cause`/`errors` are
      // deliberately NOT reserved — see the `errors` field doc above.
      if (RESERVED_FALLBACK_KEYS.has(key)) continue;
      (this as unknown as Record<string, unknown>)[key] = value;
    }
  }
}

/** An error raised while dispatching or executing a grain call. */
export class GrainCallError extends ThreshRuntimeError {
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
export class RejectionError extends ThreshRuntimeError {
  constructor(
    message: string,
    readonly kind: RejectionKind,
  ) {
    super(message);
    this.name = "RejectionError";
  }
}

/** A grain call that did not receive a response within its deadline. */
export class GrainCallTimeoutError extends ThreshRuntimeError {
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
export class GrainExtensionNotInstalledException extends ThreshRuntimeError {
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
export class LimitExceededException extends ThreshRuntimeError {
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
export class GatewayTooBusyException extends ThreshRuntimeError {
  constructor(message = "Gateway too busy") {
    super(message);
    this.name = "GatewayTooBusyException";
  }
}

/**
 * The common base beneath the cancellation family. C#'s
 * `TaskCanceledException` derives from `OperationCanceledException`, so one
 * `catch (OperationCanceledException)` covers both; TypeScript has no such
 * hierarchy, and a DOM `AbortError` is a third shape that no class base can
 * reach. Use {@link isCancellationError} for the full predicate — this base
 * covers only the two errors Thresh itself raises.
 *
 * Deliberately NOT a {@link ThreshRuntimeError}: a cancellation is the caller
 * getting what it asked for, not a call failure, and mapping it as one turns a
 * deliberate abort into a retriable availability error.
 */
export class ThreshCancellationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ThreshCancellationError";
  }
}

/**
 * Whether `error` is a cancellation, in any of the three shapes it takes:
 * {@link GrainCallAbortedError}, {@link GrainTaskCanceledError}, and a DOM
 * `AbortError` (what `AbortSignal.throwIfAborted()` and an aborted `fetch`
 * raise). The single-predicate counterpart of a C#
 * `catch (OperationCanceledException)`.
 */
export function isCancellationError(error: unknown): boolean {
  if (error instanceof ThreshCancellationError) return true;
  // A DOMException in Node, but matched by `name` so a host that raises a
  // plain `Error` for an aborted operation is still recognised.
  return error instanceof Error && error.name === "AbortError";
}

/**
 * Raised when a cooperatively-cancelled grain call observes its
 * `GrainCancellationToken` aborted and stops itself (Orleans
 * `TaskCanceledException`). JS has no thread interruption, so this is thrown
 * by application code (typically via `GrainCancellationToken
 * .throwIfCancellationRequested()` or a cancellable await) rather than by the
 * runtime tearing down the call.
 */
export class GrainTaskCanceledError extends ThreshCancellationError {
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
export class GrainCallAbortedError extends ThreshCancellationError {
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
