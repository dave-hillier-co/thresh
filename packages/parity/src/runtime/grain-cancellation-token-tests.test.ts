// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/GrainCancellationTokenTests.cs @ v10.1.0 (MIT).
// Every test drives a grain method through a GrainCancellationTokenSource, an
// Orleans-specific cooperative-cancellation mechanism with no equivalent here.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.CancellationTests.GrainCancellationTokenTests", () => {
  const testClass = "UnitTests.CancellationTests.GrainCancellationTokenTests";

  orleansTest.gap("GAP-CANCELLATION", `${testClass}.GrainTaskCancellation`);
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.MultipleGrainsTaskCancellation`);
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.GrainTaskMultipleCancellations`);
  orleansTest.gap(
    "GAP-CANCELLATION",
    `${testClass}.TokenPassingWithoutCancellation_NoExceptionShouldBeThrown`,
  );
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.PreCancelledTokenPassing`);
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.CancellationTokenCallbacksExecutionContext`);
  orleansTest.gap(
    "GAP-CANCELLATION",
    `${testClass}.CancellationTokenCallbacksTaskSchedulerContext`,
  );
  orleansTest.gap(
    "GAP-CANCELLATION",
    `${testClass}.CancellationTokenCallbacksThrow_ExceptionShouldBePropagated`,
  );
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.InSiloGrainCancellation`);
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.InterSiloGrainCancellation`);
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.InterSiloClientCancellationTokenPassing`);
  orleansTest.gap("GAP-CANCELLATION", `${testClass}.InSiloClientCancellationTokenPassing`);
});
