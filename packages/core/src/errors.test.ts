import { describe, expect, it } from "vitest";
import {
  GatewayTooBusyException,
  GrainCallAbortedError,
  GrainCallError,
  GrainCallTimeoutError,
  GrainExtensionNotInstalledException,
  GrainTaskCanceledError,
  LimitExceededException,
  RejectionError,
  ThreshCancellationError,
  ThreshRuntimeError,
  isCancellationError,
  isThreshRuntimeError,
} from "@thresh/core/errors";

// Orleans has `OrleansException` as the catch-all base beneath the transport
// family (`SiloUnavailableException`, `OrleansMessageRejectionException`,
// `TimeoutException`), so a transliterated `catch (OrleansException)` needs a
// single Thresh predicate rather than an open-coded list that every new error
// type silently falls through.
describe("ThreshRuntimeError", () => {
  it("is the common base of the grain-call failure family", () => {
    expect(new GrainCallError("boom")).toBeInstanceOf(ThreshRuntimeError);
    expect(new RejectionError("nope", "overloaded")).toBeInstanceOf(ThreshRuntimeError);
    expect(new GrainCallTimeoutError("late")).toBeInstanceOf(ThreshRuntimeError);
  });

  it("answers 'is this any runtime call failure?' in one predicate", () => {
    expect(isThreshRuntimeError(new GrainCallError("boom"))).toBe(true);
    expect(isThreshRuntimeError(new RejectionError("nope", "staleView"))).toBe(true);
    expect(isThreshRuntimeError(new GrainCallTimeoutError("late"))).toBe(true);
  });

  /**
   * Orleans derives all three of these from `OrleansException`, so a C# `catch (OrleansException)`
   * catches them and the single predicate that stands in for it must too. Two are retriable
   * load-shedding faults raised on live paths - `GatewayTooBusyException` at the gateway
   * (`cluster-node.ts`, `client-node.ts`) and `LimitExceededException` on queue overflow
   * (`turn-scheduler.ts`) - so a consumer that misses them stops retrying exactly where Orleans
   * would retry.
   */
  it("covers every error whose Orleans namesake derives from OrleansException", () => {
    expect(isThreshRuntimeError(new GatewayTooBusyException("gateway overloaded"))).toBe(true);
    expect(isThreshRuntimeError(new LimitExceededException("inbound queue", 101, 100))).toBe(true);
    expect(
      isThreshRuntimeError(new GrainExtensionNotInstalledException("no extension bound")),
    ).toBe(true);
  });

  it("does not swallow programming faults or arbitrary throws", () => {
    // The `_ => false` default arm consumers rely on: a TypeError must never be
    // reported as a retriable transport failure.
    expect(isThreshRuntimeError(new TypeError("bug"))).toBe(false);
    expect(isThreshRuntimeError(new RangeError("bug"))).toBe(false);
    expect(isThreshRuntimeError(new Error("plain"))).toBe(false);
    expect(isThreshRuntimeError("not an error")).toBe(false);
    expect(isThreshRuntimeError(undefined)).toBe(false);
  });

  it("leaves the leaf classes distinguishable — the base widens nothing existing", () => {
    // Blast-radius guard: a base ABOVE GrainCallError must not make a
    // RejectionError satisfy `instanceof GrainCallError`, which would silently
    // weaken every existing narrowing (e.g. the UNREGISTERED degradation path
    // in `packages/runtime/src/cluster.error-fidelity.test.ts`).
    expect(new RejectionError("nope", "noActivation")).not.toBeInstanceOf(GrainCallError);
    expect(new GrainCallTimeoutError("late")).not.toBeInstanceOf(GrainCallError);
    expect(new GrainCallError("boom")).not.toBeInstanceOf(RejectionError);
    expect(new GrainCallError("boom")).not.toBeInstanceOf(GrainCallTimeoutError);
  });

  it("keeps each leaf's own name and carried state", () => {
    expect(new GrainCallError("boom").name).toBe("GrainCallError");
    const rejection = new RejectionError("nope", "siloDraining");
    expect(rejection.name).toBe("RejectionError");
    expect(rejection.kind).toBe("siloDraining");
    expect(new GrainCallTimeoutError("late").name).toBe("GrainCallTimeoutError");
    const cause = new Error("inner");
    expect(new GrainCallError("boom", { cause }).cause).toBe(cause);
  });
});

// C#'s `TaskCanceledException` derives from `OperationCanceledException`, so one
// `catch (OperationCanceledException)` covers both. TypeScript has no such
// hierarchy, and a DOM `AbortError` is a third shape entirely.
describe("cancellation family", () => {
  it("gives the two Thresh cancellation errors a common base", () => {
    expect(new GrainCallAbortedError()).toBeInstanceOf(ThreshCancellationError);
    expect(new GrainTaskCanceledError()).toBeInstanceOf(ThreshCancellationError);
  });

  it("recognises all three cancellation shapes in one predicate", () => {
    expect(isCancellationError(new GrainCallAbortedError())).toBe(true);
    expect(isCancellationError(new GrainTaskCanceledError())).toBe(true);
    const controller = new AbortController();
    controller.abort();
    let aborted: unknown;
    try {
      controller.signal.throwIfAborted();
    } catch (error) {
      aborted = error;
    }
    expect(isCancellationError(aborted)).toBe(true);
  });

  it("does not classify a cancellation as a runtime call failure, or vice versa", () => {
    // Orleans' `OperationCanceledException` is not an `OrleansException`, and a
    // consumer classifies cancellation BEFORE transport: conflating the two
    // would turn a caller cancellation into a retriable unavailable.
    expect(isThreshRuntimeError(new GrainCallAbortedError())).toBe(false);
    expect(isThreshRuntimeError(new GrainTaskCanceledError())).toBe(false);
    expect(isCancellationError(new GrainCallError("boom"))).toBe(false);
    expect(isCancellationError(new GrainCallTimeoutError("late"))).toBe(false);
    expect(isCancellationError(new TypeError("bug"))).toBe(false);
  });

  it("keeps the cancellation leaves distinguishable from each other", () => {
    expect(new GrainCallAbortedError()).not.toBeInstanceOf(GrainTaskCanceledError);
    expect(new GrainTaskCanceledError()).not.toBeInstanceOf(GrainCallAbortedError);
    expect(new GrainCallAbortedError().name).toBe("GrainCallAbortedError");
    expect(new GrainTaskCanceledError().name).toBe("GrainTaskCanceledError");
  });
});
