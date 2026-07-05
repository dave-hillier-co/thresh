// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/CancellationTokenTests.cs @ v10.1.0 (MIT).
// Upstream is an abstract base class run twice via two IClassFixture subclasses
// (CancellationTokenTests_WaitForAcknowledgement / _NoWaitForAcknowledgement),
// each exercising every method with SiloMessagingOptions.WaitForCancellationAcknowledgement
// set true/false respectively. Every test passes a CancellationToken as a grain-call
// argument and expects mid-call cancellation to propagate as an exception; this
// framework has no cancellation-token/response-timeout mechanism at all.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.CancellationTests.CancellationTokenTests", () => {
  for (const testClass of [
    "UnitTests.CancellationTests.CancellationTokenTests_WaitForAcknowledgement",
    "UnitTests.CancellationTests.CancellationTokenTests_NoWaitForAcknowledgement",
  ]) {
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
      `${testClass}.CancellationTokenCallbacksThrow_ExceptionDoesNotPropagate`,
    );
    orleansTest.gap("GAP-CANCELLATION", `${testClass}.InSiloCancellation`);
    orleansTest.gap("GAP-CANCELLATION", `${testClass}.InterSiloCancellation`);
    orleansTest.gap("GAP-CANCELLATION", `${testClass}.InterSiloClientCancellationTokenPassing`);
    orleansTest.gap("GAP-CANCELLATION", `${testClass}.InSiloClientCancellationTokenPassing`);
    orleansTest.gap("GAP-CANCELLATION", `${testClass}.InterleavingGrainTaskCancellation`);
    orleansTest.gap(
      "GAP-CANCELLATION",
      `${testClass}.CancelInterleavingWhileRegularGrainRequestRunning`,
    );
    orleansTest.gap(
      "GAP-CANCELLATION",
      `${testClass}.MultipleInterleavingGrainRequestsCancellation`,
    );
  }
});
