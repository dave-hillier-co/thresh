import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import {
  GrainCallAbortedError,
  GrainCallError,
  GrainCallTimeoutError,
  GrainTaskCanceledError,
  isCancellationError,
  isThreshRuntimeError,
  UnavailableExceptionFallbackException,
} from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { registerSurrogate } from "@thresh/core/value-codec";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/**
 * A domain error the application has taught the codec about, the way Orleans applications put
 * `[GenerateSerializer]` on an exception type so it crosses the silo boundary with its concrete
 * type intact. Without that fidelity a caller cannot branch on WHICH failure occurred, only on the
 * message text -- which is presentation, not contract.
 */
class QuotaExceededError extends Error {
  constructor(
    message: string,
    readonly limit: number,
  ) {
    super(message);
    this.name = "QuotaExceededError";
  }
}

registerSurrogate<QuotaExceededError>({
  tag: "test.quotaExceededError",
  test: (value) => value instanceof QuotaExceededError,
  encode: (error) => ({ message: error.message, limit: error.limit }),
  decode: (fields) => new QuotaExceededError(fields.message as string, fields.limit as number),
});

/**
 * An error with NO surrogate — the shape issue #61 is about. It must still reach the caller with
 * enough of itself to be discriminated on: the type name and any carried state.
 */
class UnregisteredError extends Error {
  constructor(
    message: string,
    readonly limit: number,
  ) {
    super(message);
    this.name = "UnregisteredError";
  }
}

interface IThrowingGrain extends GrainWithStringKey {
  throwRegistered(): Promise<void>;
  throwUnregistered(): Promise<void>;
  throwAborted(): Promise<void>;
  throwCallAborted(): Promise<void>;
  throwTaskCanceled(): Promise<void>;
  throwRuntimeError(): Promise<void>;
  throwWithCause(): Promise<void>;
}
const IThrowingGrain = defineGrainInterface<IThrowingGrain>("IThrowingGrain.errorFidelity");

@grain()
class ThrowingGrain extends Grain implements IThrowingGrain {
  async throwRegistered(): Promise<void> {
    throw new QuotaExceededError("over the limit", 42);
  }
  async throwUnregistered(): Promise<void> {
    throw new UnregisteredError("no surrogate for this one", 42);
  }
  async throwCallAborted(): Promise<void> {
    // What `raceSignal` (`@thresh/core/abort`) raises when the awaited operation's signal fires -
    // the shape a ported `Task.Delay(ms, ct)` takes.
    throw new GrainCallAbortedError();
  }
  async throwTaskCanceled(): Promise<void> {
    throw new GrainTaskCanceledError();
  }
  async throwRuntimeError(): Promise<void> {
    throw new GrainCallTimeoutError("too slow");
  }
  async throwAborted(): Promise<void> {
    // Exactly what `signal.throwIfAborted()` raises when a callee stops because its cancellation
    // signal fired: a built-in `DOMException`, not an application error type anyone can register.
    AbortSignal.abort().throwIfAborted();
  }
  async throwWithCause(): Promise<void> {
    function deepInsideTheCallee(): never {
      throw new RangeError("root");
    }
    let root: RangeError;
    try {
      deepInsideTheCallee();
      throw new Error("unreachable");
    } catch (e) {
      root = e as RangeError;
    }
    throw new GrainCallError("outer", { cause: root });
  }
}

/** Two silos, deterministic placement: `random -> 0` hosts on silo-0, so silo-1 calls cross-wire. */
function buildCluster() {
  return TestCluster.start({
    clusterId: "error-fidelity-cluster",
    initialSilos: 2,
    random: () => 0,
    grains: [{ ctor: ThrowingGrain, interfaces: [IThrowingGrain] }],
  });
}

describe("cross-silo error type fidelity", () => {
  it("preserves the concrete type of an error whose surrogate is registered", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const call = caller.host.getGrain(IThrowingGrain, "g1").throwRegistered();

      const error = await call.then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(error).toBeInstanceOf(QuotaExceededError);
      expect((error as QuotaExceededError).limit).toBe(42);
      expect((error as QuotaExceededError).message).toBe("over the limit");
    } finally {
      await cluster.dispose();
    }
  });

  it("preserves a DOMException, so a cancelled callee reaches the caller AS a cancellation", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g3")
        .throwAborted()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      // A caller that has to read the message text to know it was cancelled cannot tell a
      // cancellation from any other failure, so the concrete type is the contract here.
      expect(error).toBeInstanceOf(DOMException);
      expect((error as DOMException).name).toBe("AbortError");
    } finally {
      await cluster.dispose();
    }
  });

  it("preserves a GrainCallAbortedError, so raceSignal's cancellation stays a cancellation", async () => {
    // Cancellation is a FAMILY, and a caller distinguishes it from a failure by type: degrading it
    // to `GrainCallError` makes `isCancellationError` false and a deliberate abort indistinguishable
    // from a retriable call failure.
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g4")
        .throwCallAborted()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      expect(error).toBeInstanceOf(GrainCallAbortedError);
      expect(isCancellationError(error)).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });

  it("preserves a GrainTaskCanceledError across the same path", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g5")
        .throwTaskCanceled()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      expect(error).toBeInstanceOf(GrainTaskCanceledError);
      expect(isCancellationError(error)).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });

  it("carries an unregistered error's name and carried state, so the caller can still discriminate", async () => {
    // This used to arrive as a bare `GrainCallError` carrying only the message: the codec's
    // generic object branch flattened an `Error` subclass (no enumerable own properties) to `{}`,
    // so the type the caller discriminates on was gone and nothing warned. Orleans' answer is
    // `UnavailableExceptionFallbackException`, which carries the type name and the properties.
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g2")
        .throwUnregistered()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      expect(error).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect((error as Error).name).toBe("UnregisteredError");
      expect((error as Error).message).toBe("no surrogate for this one");
      expect((error as UnavailableExceptionFallbackException).errorType).toBe("UnregisteredError");
      expect((error as unknown as UnregisteredError).limit).toBe(42);
    } finally {
      await cluster.dispose();
    }
  });

  it("does not report an unregistered domain error as a Thresh transport failure", async () => {
    // The half of the bug that changed a gRPC status code: a consumer classifying "is this a
    // retriable transport fault?" asks `isThreshRuntimeError` / `instanceof GrainCallError`. A
    // domain error that degraded to `GrainCallError` answered YES, and a permanent domain failure
    // was retried. Orleans' fallback is a plain `Exception`, not an `OrleansException` — so is
    // this one.
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g6")
        .throwUnregistered()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      expect(error).not.toBeInstanceOf(GrainCallError);
      expect(isThreshRuntimeError(error)).toBe(false);
    } finally {
      await cluster.dispose();
    }
  });

  it("keeps a Thresh runtime error as its own class across the wire", async () => {
    // The runtime's own family is the analogue of Orleans resolving an exception type it knows:
    // a `catch (OrleansException)` ported as `isThreshRuntimeError` must keep firing cross-silo.
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g7")
        .throwRuntimeError()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      expect(error).toBeInstanceOf(GrainCallTimeoutError);
      expect(isThreshRuntimeError(error)).toBe(true);
      expect((error as Error).message).toBe("too slow");
    } finally {
      await cluster.dispose();
    }
  });

  it("carries an error's cause and remote stack trace across a real grain call", async () => {
    // Split out of #61 as issue #63: `cause` is a non-enumerable spec property, so the generic
    // `error` envelope never carried it, and a wrapped root cause silently vanished at the silo
    // boundary. This exercises the fix through a real cross-silo call, not just the codec directly.
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const error = await caller.host
        .getGrain(IThrowingGrain, "g8")
        .throwWithCause()
        .then(
          () => undefined,
          (e: unknown) => e,
        );
      expect(error).toBeInstanceOf(GrainCallError);
      expect((error as GrainCallError).message).toBe("outer");
      expect((error as GrainCallError).cause).toBeInstanceOf(RangeError);
      expect(((error as GrainCallError).cause as RangeError).message).toBe("root");
      expect((error as Error).stack).toContain("throwWithCause");
      expect((error as Error).stack).toContain("--- End of remote stack trace from grain call ---");
    } finally {
      await cluster.dispose();
    }
  });
});
