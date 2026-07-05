// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/GrainPlacementFilterTests.cs @ v10.1.0 (MIT).
//
// Exercises Orleans' custom placement-filter extensibility surface:
// `IServiceCollection.AddPlacementFilter<TStrategy, TDirector>`, a per-grain
// `[XyzPlacementFilter(order)]` attribute stack that composes multiple
// independently-registered `IPlacementFilterDirector`s in `order` sequence
// (both ascending and interleaved orderings), and an
// `InvalidOperationException` thrown when two filters on the same grain
// declare a duplicate `order`. This framework's placement-filter mechanism
// (`PlacementFilterDescriptor`/`PlacementFilter`, see
// `@tsva/runtime/placement/placement-filter`) is a fixed, closed set resolved
// from `GrainOptions.placementFilters` at grain-registration time: there is
// no DI-based filter-strategy registration, no per-grain attribute stacking,
// no ordering (and hence no duplicate-order detection), and no way for a test
// to observe individual filter invocation via a semaphore
// (GAP-PLACEMENT-FILTER-DIRECTORS).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.PlacementFilterTests.GrainPlacementFilterTests", () => {
  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_GrainWithoutFilterCanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_FilterIsTriggered",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderAB12",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderAB21",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderBA12",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderBA21",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_DuplicateOrder",
  );
});
