// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/StatelessWorkerTests.cs @ v10.1.0 (MIT).
//
// Every case here needs a capability this framework does not have: enforced
// [StatelessWorker] multi-activation scaling per silo (GAP-STATELESS-WORKER —
// placement is parsed but not enforced, so activation-count assertions cannot
// hold), the [MayInterleave] predicate (GAP-MAY-INTERLEAVE), or (for the
// constructor-throw case) .NET's specific "Failed to create an instance of
// grain type" wrapping with an `InnerException`, which this framework's plain
// activation-error propagation does not replicate.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.General.StatelessWorkerTests", () => {
  orleansTest.excluded(
    '.NET-specific activation-failure wrapping: "Failed to create an instance of grain type" ' +
      "with a nested InnerException has no analogue in this framework's plain constructor-error propagation",
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorkerThrowExceptionConstructor",
  );

  orleansTest.gap(
    "GAP-STATELESS-WORKER",
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorkerActivationsPerSiloDoNotExceedMaxLocalWorkersCount",
  );

  orleansTest.gap(
    "GAP-STATELESS-WORKER",
    "DefaultCluster.Tests.General.StatelessWorkerTests.ManyConcurrentInvocationsOnActivationLimitedStatelessWorkerDoesNotFail",
  );

  orleansTest.excluded(
    'skipped upstream: "Skipping test for now, since there seems to be a bug" (SkippableFact Skip=...)',
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorkerFastActivationsDontFailInMultiSiloDeployment",
  );

  orleansTest.gap(
    "GAP-MAY-INTERLEAVE",
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorker_DoesNotThrow_IfMarkedWithMayInterleave",
  );

  orleansTest.gap(
    "GAP-MAY-INTERLEAVE",
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorker_ShouldNotInterleaveCalls_IfMayInterleavePredicatedDoesntMatch",
  );

  orleansTest.gap(
    "GAP-MAY-INTERLEAVE",
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorker_ShouldInterleaveCalls_IfMayInterleavePredicatedMatches",
  );
});
