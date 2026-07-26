// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/GrainPlacementFilterTests.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  ITestAB12FilteredGrain,
  ITestAB21FilteredGrain,
  ITestBA12FilteredGrain,
  ITestBA21FilteredGrain,
  ITestDuplicateOrderFilteredGrain,
  ITestFilteredGrain,
  IUnfilteredGrain,
} from "@thresh/parity/grains/interfaces/placement-filter-test-grain-interfaces";

export {
  ITestAB12FilteredGrain,
  ITestAB21FilteredGrain,
  ITestBA12FilteredGrain,
  ITestBA21FilteredGrain,
  ITestDuplicateOrderFilteredGrain,
  ITestFilteredGrain,
  IUnfilteredGrain,
};

@grain({ name: "UnitTests.PlacementFilterTests.UnfilteredGrain" })
export class UnfilteredGrain extends Grain implements IUnfilteredGrain {
  async ping(): Promise<void> {}
}

/** Names the custom directors `TestCluster`'s `configureSilo` registers via `builder.addPlacementFilter(...)`. */
export const TEST_PLACEMENT_FILTER = "TestPlacementFilter";
export const ORDER_A_PLACEMENT_FILTER = "OrderAPlacementFilter";
export const ORDER_B_PLACEMENT_FILTER = "OrderBPlacementFilter";

@grain({
  name: "UnitTests.PlacementFilterTests.TestFilteredGrain",
  placementFilters: [{ kind: "custom", name: TEST_PLACEMENT_FILTER, order: 1 }],
})
export class TestFilteredGrain extends Grain implements ITestFilteredGrain {
  async ping(): Promise<void> {}
}

@grain({
  name: "UnitTests.PlacementFilterTests.TestAB12FilteredGrain",
  placementFilters: [
    { kind: "custom", name: ORDER_A_PLACEMENT_FILTER, order: 1 },
    { kind: "custom", name: ORDER_B_PLACEMENT_FILTER, order: 2 },
  ],
})
export class TestAB12FilteredGrain extends Grain implements ITestAB12FilteredGrain {
  async ping(): Promise<void> {}
}

@grain({
  name: "UnitTests.PlacementFilterTests.TestAB21FilteredGrain",
  placementFilters: [
    { kind: "custom", name: ORDER_A_PLACEMENT_FILTER, order: 2 },
    { kind: "custom", name: ORDER_B_PLACEMENT_FILTER, order: 1 },
  ],
})
export class TestAB21FilteredGrain extends Grain implements ITestAB21FilteredGrain {
  async ping(): Promise<void> {}
}

@grain({
  name: "UnitTests.PlacementFilterTests.TestBA12FilteredGrain",
  placementFilters: [
    { kind: "custom", name: ORDER_B_PLACEMENT_FILTER, order: 1 },
    { kind: "custom", name: ORDER_A_PLACEMENT_FILTER, order: 2 },
  ],
})
export class TestBA12FilteredGrain extends Grain implements ITestBA12FilteredGrain {
  async ping(): Promise<void> {}
}

@grain({
  name: "UnitTests.PlacementFilterTests.TestBA21FilteredGrain",
  placementFilters: [
    { kind: "custom", name: ORDER_B_PLACEMENT_FILTER, order: 2 },
    { kind: "custom", name: ORDER_A_PLACEMENT_FILTER, order: 1 },
  ],
})
export class TestBA21FilteredGrain extends Grain implements ITestBA21FilteredGrain {
  async ping(): Promise<void> {}
}

@grain({
  name: "UnitTests.PlacementFilterTests.TestDuplicateOrderFilteredGrain",
  placementFilters: [
    { kind: "custom", name: ORDER_B_PLACEMENT_FILTER, order: 2 },
    { kind: "custom", name: ORDER_A_PLACEMENT_FILTER, order: 2 },
  ],
})
export class TestDuplicateOrderFilteredGrain
  extends Grain
  implements ITestDuplicateOrderFilteredGrain
{
  async ping(): Promise<void> {}
}
