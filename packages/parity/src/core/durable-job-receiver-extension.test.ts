// Ported from dotnet/orleans test/Orleans.Core.Tests/DurableJobs/DurableJobReceiverExtensionTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.ScheduledJobs.DurableJobReceiverExtensionTests", () => {
  orleansTest.excluded(
    ".NET-specific mechanism: DurableJobReceiverExtension is a separate IGrainExtension " +
      "adapter that threads a CancellationToken through job execution and distinguishes " +
      "OperationCanceledException from an ordinary failure. This framework dispatches " +
      "DURABLE_JOB_HANDLER directly (no extension layer, no CancellationToken) — a job's " +
      "run is just a Promise<DurableJobRunResult>, so there is no equivalent propagation path.",
    "NonSilo.Tests.ScheduledJobs.DurableJobReceiverExtensionTests.HandleDurableJobAsync_WhenExecutionTaskIsCanceled_PropagatesCancellation",
  );

  orleansTest.excluded(
    ".NET-specific mechanism: the extension's 'IsPending' re-entry semantics for an " +
      "already-canceled token whose execution Task is still running has no analog — this " +
      "framework has no per-call CancellationToken or a HandleDurableJobAsync entry point " +
      "that can be called twice concurrently for the same job to observe a pending flag.",
    "NonSilo.Tests.ScheduledJobs.DurableJobReceiverExtensionTests.HandleDurableJobAsync_WhenTokenIsCanceledButExecutionIsStillRunning_RemainsPending",
  );

  orleansTest.excluded(
    ".NET-specific null-argument guard: DurableJobRunResult.Failed(null!) throwing " +
      "ArgumentNullException tests .NET nullability enforcement on a static factory. This " +
      "framework's `failed(error)` accepts `unknown` (including null/undefined) by design — " +
      "there is no equivalent null-check to exercise.",
    "NonSilo.Tests.ScheduledJobs.DurableJobReceiverExtensionTests.DurableJobRunResult_Failed_ThrowsForNullException",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
