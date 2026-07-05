// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/CountersGrainTests.cs @ v10.1.0 (MIT).
// `ICountersGrain`'s tentative-vs-confirmed count split (`GetTentativeState`
// visible immediately, `GetConfirmedState` only after
// `ConfirmAllPreviouslyRaisedEvents`) is the log-consistency protocol itself —
// there is no journaled-grain / log-consistency-provider mechanism in this
// framework (GAP-EVENT-SOURCING), so the whole file is gap-tagged.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.EventSourcingTests.CountersGrainTests", () => {
  orleansTest.gap("GAP-EVENT-SOURCING", "Tester.EventSourcingTests.CountersGrainTests.Record");
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.ConcurrentIncrements",
  );
});
