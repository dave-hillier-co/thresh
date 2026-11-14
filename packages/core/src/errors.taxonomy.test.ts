import { describe, expect, it } from "vitest";
import * as errors from "@thresh/core/errors";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";

type ErrorClass = abstract new (...args: never[]) => Error;

/**
 * A way to build one instance of every error class `errors.ts` exports. The enumeration test below
 * fails when a class is exported without an entry here, so a new class cannot be added without
 * also being proven to cross the wire as itself.
 */
const factories: Record<string, () => Error> = {
  ThreshRuntimeError: () => new errors.ThreshRuntimeError("runtime"),
  ThreshTransactionError: () => new errors.ThreshTransactionError("tx"),
  UnavailableExceptionFallbackException: () =>
    new errors.UnavailableExceptionFallbackException("SomeRemoteError", "fallback", { a: 1 }),
  GrainCallError: () => new errors.GrainCallError("call"),
  RejectionError: () => new errors.RejectionError("rejected", "overloaded"),
  GrainCallTimeoutError: () => new errors.GrainCallTimeoutError("timeout"),
  GrainExtensionNotInstalledException: () =>
    new errors.GrainExtensionNotInstalledException("no extension"),
  InconsistentStateError: () => new errors.InconsistentStateError("etag", "1", "2"),
  TransactionsDisabledError: () => new errors.TransactionsDisabledError(),
  LimitExceededException: () => new errors.LimitExceededException("queue", 5, 4),
  GatewayTooBusyException: () => new errors.GatewayTooBusyException("busy"),
  ThreshCancellationError: () => new errors.ThreshCancellationError("cancelled"),
  GrainTaskCanceledError: () => new errors.GrainTaskCanceledError("task"),
  GrainCallAbortedError: () => new errors.GrainCallAbortedError("aborted"),
  TransactionAbortedError: () => new errors.TransactionAbortedError("tx1", "why"),
  TransactionAlreadyResolvedError: () =>
    new errors.TransactionAlreadyResolvedError("tx1", "committed"),
  TransactionReadOnlyViolatedError: () => new errors.TransactionReadOnlyViolatedError("tx1"),
  TransactionLockUpgradeError: () => new errors.TransactionLockUpgradeError("tx1"),
  TransactionBrokenLockError: () =>
    new errors.TransactionBrokenLockError("tx1", "when re-entering lock"),
  TransactionOrphanCallError: () => new errors.TransactionOrphanCallError("tx1", 2),
  ReadOnlyStateViolationError: () => new errors.ReadOnlyStateViolationError("read only"),
  TransactionCascadingAbortError: () => new errors.TransactionCascadingAbortError("tx1"),
  TransactionInDoubtError: () => new errors.TransactionInDoubtError("tx1"),
};

const exportedClasses: Array<[string, ErrorClass]> = (
  Object.entries(errors) as Array<[string, unknown]>
)
  .filter(
    (entry): entry is [string, ErrorClass] =>
      typeof entry[1] === "function" && entry[1].prototype instanceof Error,
  )
  .sort(([a], [b]) => a.localeCompare(b));

function basesOf(ctor: ErrorClass): ErrorClass[] {
  const bases: ErrorClass[] = [];
  for (
    let base = Object.getPrototypeOf(ctor) as ErrorClass;
    base.prototype instanceof Error;
    base = Object.getPrototypeOf(base) as ErrorClass
  ) {
    bases.push(base);
  }
  return bases;
}

describe("error taxonomy", () => {
  it("has a factory for every Error subclass errors.ts exports, and no stale ones", () => {
    expect(Object.keys(factories).sort()).toEqual(exportedClasses.map(([name]) => name));
  });

  describe.each(exportedClasses)("%s", (name, ctor) => {
    it("round-trips through the codec as itself and as each of its bases", () => {
      const original = factories[name]!();
      const back = deserializeValue<Error>(serializeValue(original));

      expect(back).toBeInstanceOf(ctor);
      for (const base of basesOf(ctor)) expect(back).toBeInstanceOf(base);
      expect(back.message).toBe(original.message);
    });
  });

  it("rebuilds TransactionAlreadyResolvedError with its status, as a TransactionAbortedError", () => {
    const back = deserializeValue<errors.TransactionAlreadyResolvedError>(
      serializeValue(new errors.TransactionAlreadyResolvedError("tx9", "resolving")),
    );
    expect(back).toBeInstanceOf(errors.TransactionAbortedError);
    expect(back.transactionId).toBe("tx9");
    expect(back.status).toBe("resolving");
  });

  // isThreshRuntimeError stands in for `catch (OrleansException)`. Each row names the Orleans
  // class and file that fixes the expected answer.
  describe("isThreshRuntimeError mirrors the OrleansException hierarchy", () => {
    const orleansCore = "src/Orleans.Core/Providers/IGrainStorage.cs";
    const orleansTx = "src/Orleans.Transactions/OrleansTransactionException.cs";
    const rows: Array<[string, boolean, string]> = [
      [
        "InconsistentStateError",
        true,
        `InconsistentStateException : OrleansException (${orleansCore})`,
      ],
      [
        "TransactionsDisabledError",
        true,
        `OrleansTransactionsDisabledException : OrleansTransactionException (${orleansTx})`,
      ],
      [
        "TransactionInDoubtError",
        true,
        `OrleansTransactionInDoubtException : OrleansTransactionException (${orleansTx})`,
      ],
      [
        "TransactionAbortedError",
        true,
        `OrleansTransactionAbortedException : OrleansTransactionException (${orleansTx})`,
      ],
      [
        "TransactionAlreadyResolvedError",
        true,
        `subtype of OrleansTransactionAbortedException (${orleansTx})`,
      ],
      [
        "TransactionReadOnlyViolatedError",
        true,
        `OrleansReadOnlyViolatedException : OrleansTransactionAbortedException (${orleansTx})`,
      ],
      [
        "TransactionLockUpgradeError",
        true,
        `OrleansTransactionLockUpgradeException : ...TransientFailureException (${orleansTx})`,
      ],
      [
        "TransactionBrokenLockError",
        true,
        `OrleansBrokenTransactionLockException : ...TransientFailureException (${orleansTx})`,
      ],
      [
        "TransactionOrphanCallError",
        true,
        `OrleansOrphanCallException : OrleansTransactionAbortedException (${orleansTx})`,
      ],
      [
        "TransactionCascadingAbortError",
        true,
        `OrleansCascadingAbortException : ...TransientFailureException (${orleansTx})`,
      ],
      ["GrainCallError", true, "existing: OrleansException family"],
      ["RejectionError", true, "existing: OrleansMessageRejectionException"],
      ["LimitExceededException", true, "existing: LimitExceededException : OrleansException"],
      ["ThreshCancellationError", false, "OperationCanceledException is not an OrleansException"],
      [
        "ReadOnlyStateViolationError",
        false,
        "no Orleans counterpart; a dev-mode guard, not a runtime failure",
      ],
      [
        "UnavailableExceptionFallbackException",
        false,
        "derives from Exception, not OrleansException",
      ],
    ];

    it.each(rows)("%s -> %s (%s)", (name, expected) => {
      expect(errors.isThreshRuntimeError(factories[name]!())).toBe(expected);
    });

    it("still refuses programming faults", () => {
      expect(errors.isThreshRuntimeError(new TypeError("bug"))).toBe(false);
      expect(errors.isThreshRuntimeError(new Error("plain"))).toBe(false);
    });
  });
});
