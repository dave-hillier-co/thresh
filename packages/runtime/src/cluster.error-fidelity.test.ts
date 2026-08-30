import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GrainCallError } from "@thresh/core/errors";
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

/** An error with NO surrogate: it must still degrade to `GrainCallError`, never to a crash. */
class UnregisteredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnregisteredError";
  }
}

interface IThrowingGrain extends GrainWithStringKey {
  throwRegistered(): Promise<void>;
  throwUnregistered(): Promise<void>;
  throwAborted(): Promise<void>;
}
const IThrowingGrain = defineGrainInterface<IThrowingGrain>("IThrowingGrain.errorFidelity");

@grain()
class ThrowingGrain extends Grain implements IThrowingGrain {
  async throwRegistered(): Promise<void> {
    throw new QuotaExceededError("over the limit", 42);
  }
  async throwUnregistered(): Promise<void> {
    throw new UnregisteredError("no surrogate for this one");
  }
  async throwAborted(): Promise<void> {
    // Exactly what `signal.throwIfAborted()` raises when a callee stops because its cancellation
    // signal fired: a built-in `DOMException`, not an application error type anyone can register.
    AbortSignal.abort().throwIfAborted();
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

  it("still degrades an unregistered error to GrainCallError, keeping its message", async () => {
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
      expect(error).toBeInstanceOf(GrainCallError);
      expect((error as Error).message).toBe("no surrogate for this one");
    } finally {
      await cluster.dispose();
    }
  });
});
