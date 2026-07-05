// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/ResourceOptimizedPlacementOptionsTests.cs @ v10.1.0 (MIT).
//
// Upstream tests `ResourceOptimizedPlacementOptions` (weighted CPU/memory/
// available-memory/activation-count knobs, a local-silo preference margin,
// and a `ResourceOptimizedPlacementOptionsValidator` that rejects weights
// outside [0, 100]). `@tsva/runtime`'s `ResourceOptimizedPlacement` is a
// fixed activation-count-only algorithm with no configurable weights and no
// options validator (GAP-RESOURCE-OPTIMIZED-OPTIONS), so there is nothing to
// assert these constants/validation rules against.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.ResourceOptimizedPlacementOptionsTests", () => {
  orleansTest.gap(
    "GAP-RESOURCE-OPTIMIZED-OPTIONS",
    "UnitTests.General.ResourceOptimizedPlacementOptionsTests.ConstantsShouldNotChange",
  );

  orleansTest.gap(
    "GAP-RESOURCE-OPTIMIZED-OPTIONS",
    "UnitTests.General.ResourceOptimizedPlacementOptionsTests.DefaultShouldEqualConstants",
  );

  orleansTest.gap(
    "GAP-RESOURCE-OPTIMIZED-OPTIONS",
    "UnitTests.General.ResourceOptimizedPlacementOptionsTests.InvalidWeightsShouldThrow",
  );

  orleansTest.gap(
    "GAP-RESOURCE-OPTIMIZED-OPTIONS",
    "UnitTests.General.ResourceOptimizedPlacementOptionsTests.ValidWeightsShouldNotThrow",
  );
});
