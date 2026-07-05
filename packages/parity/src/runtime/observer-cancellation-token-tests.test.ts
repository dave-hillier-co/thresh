// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/ObserverCancellationTokenTests.cs @ v10.1.0 (MIT).
// Upstream is an abstract base class run twice via two IClassFixture subclasses
// (ObserverCancellationTokenTests_WaitForAcknowledgement / _NoWaitForAcknowledgement).
// Every test both creates a client object reference (GAP-OBSERVERS) and passes a
// CancellationToken across the grain boundary (GAP-CANCELLATION); this framework
// has neither.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.CancellationTests.ObserverCancellationTokenTests", () => {
  for (const testClass of [
    "UnitTests.CancellationTests.ObserverCancellationTokenTests_WaitForAcknowledgement",
    "UnitTests.CancellationTests.ObserverCancellationTokenTests_NoWaitForAcknowledgement",
  ]) {
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.ObserverTaskCancellation`);
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.PreCancelledTokenPassing`);
    orleansTest.gap(
      "GAP-OBSERVERS",
      `${testClass}.TokenPassingWithoutCancellation_NoExceptionShouldBeThrown`,
    );
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.CancellationTokenCallbacksExecutionContext`);
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.MultipleObserversCancellation`);
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.CancelWaitingRequest`);
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.InterleavingObserverTaskCancellation`);
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.MultipleInterleavingRequestsCancellation`);
    orleansTest.gap("GAP-OBSERVERS", `${testClass}.CancelInterleavingWhileRegularRequestRunning`);
  }
});
