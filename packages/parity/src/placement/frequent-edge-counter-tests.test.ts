// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/FrequentEdgeCounterTests.cs @ v10.1.0 (MIT).
//
// Exercises `FrequentEdgeCounter`, the bounded-capacity communication-graph
// edge counter behind the repartitioner: incrementing an existing edge,
// evicting the minimum-count edge once capacity is reached, and explicit
// removal. This framework has no repartitioning subsystem at all, so
// `FrequentEdgeCounter` does not exist (GAP-ACTIVATION-REPARTITIONING, see
// `default-tolerance-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Add_ShouldIncrementCounter_WhenEdgeIsAdded",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Add_ShouldUpdateExistingCounter_WhenSameEdgeIsAddedAgain",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Add_ShouldRemoveMinCounter_WhenCapacityIsReached",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Remove_ShouldRemoveCounter_WhenEdgeIsRemoved",
  );
});
