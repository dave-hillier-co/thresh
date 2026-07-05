// Ported from dotnet/orleans test/Grains/TestGrains/ExceptionGrain.cs @ v10.1.0 (MIT).
// Upstream mirrors .NET's `InvalidOperationException`/`NullReferenceException`
// types; this framework does not model the CLR exception hierarchy (see
// echo-task-grain.ts), so plain `Error`s with the same messages stand in and
// ported assertions check message text.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { IExceptionGrain } from "@tsva/parity/grains/interfaces/exception-grain-interfaces";

export { IExceptionGrain };

@grain({ name: "UnitTests.Grains.ExceptionGrain" })
export class ExceptionGrain extends Grain implements IExceptionGrain {
  async throwsInvalidOperationException(): Promise<void> {
    throw new Error("Test exception");
  }

  async throwsNullReferenceException(): Promise<void> {
    throw new Error("null null null");
  }

  async grainCallToThrowsInvalidOperationException(otherGrainId: bigint): Promise<void> {
    const other = this.getGrain(IExceptionGrain, otherGrainId);
    await other.throwsInvalidOperationException();
  }

  // Deliberately not `async`: throws synchronously, before any Promise is
  // returned, to verify the call still surfaces as a rejected Promise to the
  // caller rather than throwing synchronously (Orleans' "faulted Task, not a
  // synchronous throw" guarantee).
  throwsSynchronousInvalidOperationException(): Promise<void> {
    throw new Error("Test exception");
  }
}
