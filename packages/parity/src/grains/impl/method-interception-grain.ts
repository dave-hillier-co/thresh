// Ported from dotnet/orleans test/Grains/TestGrains/MethodInterceptionGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { INCOMING_CALL_FILTER, type IncomingGrainCallContext } from "@tsva/core/grain-call-filter";
import {
  IGrainCallFilterTestGrain,
  IMethodInterceptionGrain,
  IOutgoingMethodInterceptionGrain,
} from "@tsva/parity/grains/interfaces/method-interception-grain-interfaces";

export { IGrainCallFilterTestGrain, IMethodInterceptionGrain, IOutgoingMethodInterceptionGrain };

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

  async [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void> {
    if (context.methodName === "grainSpecificCallFilterMarker") {
      // explicitly do not continue calling invoke()
      return;
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
