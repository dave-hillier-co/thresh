// Ported from dotnet/orleans test/Grains/TestGrains/MethodInterceptionGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { INCOMING_CALL_FILTER, type IncomingGrainCallContext } from "@tsva/core/grain-call-filter";
import { requestContext } from "@tsva/runtime/invocation-context";
import {
  IGrainCallFilterTestGrain,
  IGrainCallFilterTestGrainObserver,
  IMethodInterceptionGrain,
  IMethodInterceptionGrainObserver,
  IMyGrainExtension,
  IOutgoingMethodInterceptionGrain,
} from "@tsva/parity/grains/interfaces/method-interception-grain-interfaces";

export {
  IGrainCallFilterTestGrain,
  IGrainCallFilterTestGrainObserver,
  IMethodInterceptionGrain,
  IMethodInterceptionGrainObserver,
  IMyGrainExtension,
  IOutgoingMethodInterceptionGrain,
};

/** Upstream `MethodInterceptionGrain.MyDomainSpecificException`. */
export class MyDomainSpecificException extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MyDomainSpecificException";
  }
}

/**
 * Upstream `MethodInterceptionGrain`: implements its own incoming call filter
 * (Orleans `IIncomingGrainCallFilter` on the grain class), the grain-level
 * filter this framework mirrors via `INCOMING_CALL_FILTER`.
 *
 * Upstream also proves the filter receives the *implementation* `MethodInfo`
 * (not the interface's) by reversing `Echo`'s result only when a
 * `[MessWithResult]` attribute — only present on the implementation — is
 * found via reflection. There is no reflection distinguishing interface vs.
 * implementation methods in this framework, so that mechanism is dropped; the
 * observable behaviour (Echo's result comes back reversed) is preserved by
 * checking the method name directly.
 */
@grain({ name: "UnitTests.Grains.MethodInterceptionGrain" })
export class MethodInterceptionGrain extends Grain implements IMethodInterceptionGrain {
  async one(): Promise<string> {
    throw new Error("Not allowed to actually invoke this method!");
  }

  async echo(someArg: string): Promise<string> {
    return someArg;
  }

  async notIntercepted(): Promise<string> {
    return "not intercepted";
  }

  async sayHello(): Promise<string> {
    return "Hello";
  }

  async doThrow(): Promise<string> {
    throw new MyDomainSpecificException("Oi!");
  }

  async filterThrows(): Promise<void> {}

  async systemWideCallFilterMarker(): Promise<void> {}

  async [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void> {
    if (context.methodName === "one") {
      // Short-circuit the request and return to the caller without actually invoking the grain method.
      context.result = "intercepted one with no args";
      return;
    }

    if (context.methodName === "filterThrows") {
      throw new MyDomainSpecificException("Filter THROW!");
    }

    try {
      await context.invoke();
    } catch (error) {
      if (error instanceof MyDomainSpecificException) {
        context.result = `EXCEPTION! ${error.message}`;
        return;
      }
      throw error;
    }

    if (context.methodName === "echo" && typeof context.result === "string") {
      context.result = [...context.result].reverse().join("");
    }
  }
}

/**
 * Upstream `MethodInterceptionGrainObserver`: the observer-side mirror of
 * `MethodInterceptionGrain` — identical self-filtering behaviour, but hosted
 * as a plain object via `ClientNode.createObjectReference` rather than a
 * grain activation, so it does not extend `Grain`.
 */
export class MethodInterceptionGrainObserver implements IMethodInterceptionGrainObserver {
  async one(): Promise<string> {
    throw new Error("Not allowed to actually invoke this method!");
  }

  async echo(someArg: string): Promise<string> {
    return someArg;
  }

  async notIntercepted(): Promise<string> {
    return "not intercepted";
  }

  async sayHello(): Promise<string> {
    return "Hello";
  }

  async doThrow(): Promise<string> {
    throw new MyDomainSpecificException("Oi!");
  }

  async filterThrows(): Promise<void> {}

  async systemWideCallFilterMarker(): Promise<void> {}

  async [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void> {
    if (context.methodName === "one") {
      context.result = "intercepted one with no args";
      return;
    }

    if (context.methodName === "filterThrows") {
      throw new MyDomainSpecificException("Filter THROW!");
    }

    try {
      await context.invoke();
    } catch (error) {
      if (error instanceof MyDomainSpecificException) {
        context.result = `EXCEPTION! ${error.message}`;
        return;
      }
      throw error;
    }

    if (context.methodName === "echo" && typeof context.result === "string") {
      context.result = [...context.result].reverse().join("");
    }
  }
}

/** Upstream `OutgoingMethodInterceptionGrain`. */
@grain({ name: "UnitTests.Grains.OutgoingMethodInterceptionGrain" })
export class OutgoingMethodInterceptionGrain
  extends Grain
  implements IOutgoingMethodInterceptionGrain
{
  async throwIfGreaterThanZero(value: number): Promise<string> {
    if (value > 0) {
      throw new RangeError(`${value} is greater than zero!`);
    }
    return "Thanks for nothing";
  }

  async echoViaOtherGrain(
    otherGrain: IMethodInterceptionGrain,
    message: string,
  ): Promise<Record<string, unknown>> {
    return { result: await otherGrain.echo(message) };
  }
}

/**
 * Upstream `GrainCallFilterTestGrain`: a self-filtering grain whose own
 * incoming filter retries `throwIfGreaterThanZero` once (decrementing the
 * argument) and marks `grainSpecificCallFilterMarker` as never invoked.
 */
@grain({ name: "UnitTests.Grains.GrainCallFilterTestGrain" })
export class GrainCallFilterTestGrain extends Grain implements IGrainCallFilterTestGrain {
  async throwIfGreaterThanZero(value: number): Promise<string> {
    if (value > 0) {
      throw new RangeError(`${value} is greater than zero!`);
    }
    return "Thanks for nothing";
  }

  async sumSet(numbers: readonly number[]): Promise<number> {
    let total = 0;
    for (const n of numbers) total += n;
    return total;
  }

  async systemWideCallFilterMarker(): Promise<void> {}

  async grainSpecificCallFilterMarker(): Promise<void> {}

  /** Upstream `GetRequestContext`: `RequestContext.Get(Key) + "4"`. */
  async getRequestContext(): Promise<string | undefined> {
    return `${this.runtime.getRequestContext(GRAIN_CALL_FILTER_TEST_KEY) ?? ""}4`;
  }

  async [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void> {
    if (context.methodName === "grainSpecificCallFilterMarker") {
      // explicitly do not continue calling invoke()
      return;
    }

    // Continue building the RequestContext value: e.g. "12" -> "123" (Orleans
    // parity: the grain's own filter runs innermost, just before the method).
    if (context.methodName === "getRequestContext") {
      const existing = context.headers[GRAIN_CALL_FILTER_TEST_KEY];
      if (existing !== undefined) context.headers[GRAIN_CALL_FILTER_TEST_KEY] = `${existing}3`;
    }

    let attemptsRemaining = 2;
    while (attemptsRemaining > 0) {
      try {
        await context.invoke();
        return;
      } catch (error) {
        const canRetry =
          attemptsRemaining > 1 &&
          error instanceof RangeError &&
          context.methodName === "throwIfGreaterThanZero" &&
          typeof context.args[0] === "number";
        if (!canRetry) throw error;
        context.args[0] = (context.args[0] as number) - 1;
        attemptsRemaining -= 1;
      }
    }
  }
}

/**
 * Upstream `GrainCallFilterTestGrainObserver`: the observer-side mirror of
 * `GrainCallFilterTestGrain` — same self-filtering retry/RequestContext
 * behaviour, hosted as a plain object via `ClientNode.createObjectReference`.
 * `getRequestContext` reads back the ambient `RequestContext` value the
 * client's own incoming filters and this object's filter progressively build
 * up, via the SAME `requestContext.get` helper a grain's `runtime` uses —
 * `ClientNode.dispatchToLocalObject` establishes the ambient invocation-context
 * turn around a hosted-object call, mirroring `ActivationData.invoke`.
 */
export class GrainCallFilterTestGrainObserver implements IGrainCallFilterTestGrainObserver {
  async throwIfGreaterThanZero(value: number): Promise<string> {
    if (value > 0) {
      throw new RangeError(`${value} is greater than zero!`);
    }
    return "Thanks for nothing";
  }

  async sumSet(numbers: readonly number[]): Promise<number> {
    let total = 0;
    for (const n of numbers) total += n;
    return total;
  }

  async systemWideCallFilterMarker(): Promise<void> {}

  async grainSpecificCallFilterMarker(): Promise<void> {}

  /** Upstream `GetRequestContext`: `RequestContext.Get(Key) + "4"`. */
  async getRequestContext(): Promise<string | undefined> {
    return `${requestContext.get(GRAIN_CALL_FILTER_TEST_KEY) ?? ""}4`;
  }

  async [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void> {
    if (context.methodName === "grainSpecificCallFilterMarker") {
      // explicitly do not continue calling invoke()
      return;
    }

    // Continue building the RequestContext value: e.g. "12" -> "123" (Orleans
    // parity: the hosted object's own filter runs innermost, just before the
    // method).
    if (context.methodName === "getRequestContext") {
      const existing = context.headers[GRAIN_CALL_FILTER_TEST_KEY];
      if (existing !== undefined) context.headers[GRAIN_CALL_FILTER_TEST_KEY] = `${existing}3`;
    }

    let attemptsRemaining = 2;
    while (attemptsRemaining > 0) {
      try {
        await context.invoke();
        return;
      } catch (error) {
        const canRetry =
          attemptsRemaining > 1 &&
          error instanceof RangeError &&
          context.methodName === "throwIfGreaterThanZero" &&
          typeof context.args[0] === "number";
        if (!canRetry) throw error;
        context.args[0] = (context.args[0] as number) - 1;
        attemptsRemaining -= 1;
      }
    }
  }
}

/** Upstream `GrainCallFilterTestConstants.Key`. */
export const GRAIN_CALL_FILTER_TEST_KEY = "GrainInfo";

/**
 * Upstream `MyGrainExtension`: bound onto an `IMethodInterceptionGrain`
 * activation via `SiloBuilder.addGrainExtension` for
 * `GrainCallFilter_GrainExtension`.
 */
export class MyGrainExtension implements IMyGrainExtension {
  private value = 0;

  async setExtensionValue(value: number): Promise<void> {
    this.value = value;
  }

  async getExtensionValue(): Promise<number> {
    return this.value;
  }
}
