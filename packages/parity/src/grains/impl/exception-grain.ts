// Ported from dotnet/orleans test/Grains/TestGrains/ExceptionGrain.cs @ v10.1.0 (MIT).
// Upstream mirrors .NET's `InvalidOperationException`/`NullReferenceException`
// types; this framework does not model the CLR exception hierarchy (see
// echo-task-grain.ts), so plain `Error`s with the same messages stand in and
// ported assertions check message text.
import { grain } from "@thresh/core/decorators";
import { GrainTaskCanceledError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { IExceptionGrain } from "@thresh/parity/grains/interfaces/exception-grain-interfaces";

export { IExceptionGrain };

@grain({ name: "UnitTests.Grains.ExceptionGrain" })
export class ExceptionGrain extends Grain implements IExceptionGrain {
  // Upstream returns a canceled `Task` (`tcs.TrySetCanceled()`), which surfaces
  // to the caller as a `TaskCanceledException`. This framework has no canceled
  // Promise, so the analogue is throwing `GrainTaskCanceledError` — the same
  // error a `GrainCancellationToken` raises — which propagates as a rejection.
  async canceled(): Promise<void> {
    throw new GrainTaskCanceledError();
  }

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
