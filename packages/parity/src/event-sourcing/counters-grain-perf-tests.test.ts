// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/CountersGrainPerfTests.cs @ v10.1.0 (MIT).
// Illustrative throughput comparisons across `StorageProvider`/
// `LogConsistencyProvider` combinations (`StateStore` vs `LogStore`,
// reentrant vs non-reentrant) for a `JournaledGrain`. There is no
// journaled-grain / log-consistency-provider mechanism in this framework
// (GAP-EVENT-SOURCING), so the whole file is gap-tagged.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.EventSourcingTests.CountersGrainTests.Perf", () => {
  orleansTest.gap("GAP-EVENT-SOURCING", "Tester.EventSourcingTests.CountersGrainTests.Perf_Warmup");
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryStateStore_NonReentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryStateStore_NonReentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryLogStore_NonReentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryLogStore_NonReentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryStateStore_Reentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryStateStore_Reentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryLogStore_Reentrant",
  );
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryLogStore_Reentrant",
  );
});
