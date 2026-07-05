// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/LogTestGrainClearTests.cs @ v10.1.0 (MIT).
// Exercises `ILogTestGrain.Clear()` resetting confirmed/tentative log-consistency
// state across three storage-provider configurations (default, shared, custom
// primary-cluster), plus concurrent operations racing a clear without
// `InconsistentStateException`. This is all log-consistency-provider protocol
// behaviour; there is no journaled-grain machinery in this framework
// (GAP-EVENT-SOURCING), so the whole file is gap-tagged.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.EventSourcingTests.LogTestGrainClearTests", () => {
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.LogTestGrainClearTests.ClearLog_ResetDropsTentativeAndAllowsFurtherWrites",
  );
});
