// Ported from dotnet/orleans test/Orleans.Core.Tests/Runtime/ActivationCollectorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// This framework's ActivationCollector (packages/runtime/src/activation-collector.ts) is a
// simple periodic sweep over the catalog; it has no bucketed-by-ticket collection algorithm,
// no memory-pressure-based deactivation targets, and no ActivationWorkingSet. Every method
// below exercises internal .NET implementation details (MakeTicketFromDateTime bucketing,
// IsMemoryOverloaded threshold math, DeactivateInDueTimeOrder over NSubstitute-mocked
// ICollectibleGrainContext/IActivationWorkingSetMember) that have no counterpart here.
describe("UnitTests.Runtime.ActivationCollectorTests", () => {
  orleansTest.excluded(
    ".NET-specific: tests ActivationCollector's internal ticket-bucketing algorithm, absent from this framework's simpler timer-based collector",
    "UnitTests.Runtime.ActivationCollectorTests.MakeTicketFromDateTime",
  );

  orleansTest.excluded(
    ".NET-specific: tests ActivationCollector's internal ticket-bucketing algorithm, absent from this framework's simpler timer-based collector",
    "UnitTests.Runtime.ActivationCollectorTests.MakeTicketFromDateTime_MaxValue",
  );

  orleansTest.excluded(
    ".NET-specific: tests ActivationCollector's internal ticket-bucketing algorithm, absent from this framework's simpler timer-based collector",
    "UnitTests.Runtime.ActivationCollectorTests.MakeTicketFromDateTime_Invalid_BeforeNextTicket",
  );

  orleansTest.excluded(
    ".NET-specific: tests memory-pressure-based deactivation targeting via NSubstitute-mocked IEnvironmentStatisticsProvider, a feature this framework's collector does not implement",
    "UnitTests.Runtime.ActivationCollectorTests.IsMemoryOverloaded_WorksAsExpected",
  );

  orleansTest.excluded(
    ".NET-specific: exercises ActivationWorkingSet/ICollectibleGrainContext internals via NSubstitute mocks; no equivalent working-set abstraction exists here",
    "UnitTests.Runtime.ActivationCollectorTests.DeactivateInDueTimeOrder_OnlyOldestAndEligibleAreDeactivated",
  );

  orleansTest.excluded(
    ".NET-specific: concurrency stress test of ActivationCollector's internal bucket dictionary under multi-threaded add/remove, a threading model this framework's single-threaded event loop does not have",
    "UnitTests.Runtime.ActivationCollectorTests.DeactivateInDueTimeOrder_ConcurrentModification_ShouldNotThrow",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
