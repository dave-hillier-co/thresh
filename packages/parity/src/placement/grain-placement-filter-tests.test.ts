// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/GrainPlacementFilterTests.cs @ v10.1.0 (MIT).
//
// Exercises custom placement-filter-director registration
// (`SiloBuilder.addPlacementFilter(name, director)`, the TS analogue of
// Orleans' `AddPlacementFilter<TStrategy, TDirector>`), per-grain ordered
// filter stacking (`GrainOptions.placementFilters`, each entry's `order`
// composing ascending), duplicate-order rejection, and observable filter
// invocation.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  ORDER_A_PLACEMENT_FILTER,
  ORDER_B_PLACEMENT_FILTER,
  TEST_PLACEMENT_FILTER,
  TestAB12FilteredGrain,
  TestAB21FilteredGrain,
  TestBA12FilteredGrain,
  TestBA21FilteredGrain,
  TestDuplicateOrderFilteredGrain,
  TestFilteredGrain,
  UnfilteredGrain,
  ITestAB12FilteredGrain,
  ITestAB21FilteredGrain,
  ITestBA12FilteredGrain,
  ITestBA21FilteredGrain,
  ITestDuplicateOrderFilteredGrain,
  ITestFilteredGrain,
  IUnfilteredGrain,
} from "@tsva/parity/grains/impl/placement-filter-test-grain";
import {
  RecordingPlacementFilterDirector,
  TaggingPlacementFilterDirector,
} from "@tsva/parity/support/placement-filter-directors";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("UnitTests.PlacementFilterTests.GrainPlacementFilterTests", () => {
  let cluster: TestCluster;
  let testDirector: RecordingPlacementFilterDirector;
  let scratchpad: Map<string, string[]>;

  beforeAll(async () => {
    testDirector = new RecordingPlacementFilterDirector();
    scratchpad = new Map();
    const orderADirector = new TaggingPlacementFilterDirector("A", scratchpad);
    const orderBDirector = new TaggingPlacementFilterDirector("B", scratchpad);

    cluster = await TestCluster.start({
      initialSilos: 3,
      grains: [
        { ctor: TestFilteredGrain, interfaces: [ITestFilteredGrain] },
        { ctor: TestAB12FilteredGrain, interfaces: [ITestAB12FilteredGrain] },
        { ctor: TestAB21FilteredGrain, interfaces: [ITestAB21FilteredGrain] },
        { ctor: TestBA12FilteredGrain, interfaces: [ITestBA12FilteredGrain] },
        { ctor: TestBA21FilteredGrain, interfaces: [ITestBA21FilteredGrain] },
        { ctor: TestDuplicateOrderFilteredGrain, interfaces: [ITestDuplicateOrderFilteredGrain] },
        { ctor: UnfilteredGrain, interfaces: [IUnfilteredGrain] },
      ],
      configureSilo: (builder) => {
        builder
          .addPlacementFilter(TEST_PLACEMENT_FILTER, testDirector)
          .addPlacementFilter(ORDER_A_PLACEMENT_FILTER, orderADirector)
          .addPlacementFilter(ORDER_B_PLACEMENT_FILTER, orderBDirector);
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_GrainWithoutFilterCanBeCalled",
    async () => {
      await cluster.getGrain(IUnfilteredGrain, randomIntegerKey()).ping();
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_FilterIsTriggered",
    async () => {
      const before = testDirector.triggerCount;
      const waited = testDirector.waitForTrigger();
      await cluster.getGrain(ITestFilteredGrain, randomIntegerKey()).ping();
      expect(await waited).toBe(true);
      expect(testDirector.triggerCount).toBeGreaterThan(before);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderAB12",
    async () => {
      const key = randomIntegerKey();
      await cluster.getGrain(ITestAB12FilteredGrain, key).ping();
      const grainType = "UnitTests.PlacementFilterTests.TestAB12FilteredGrain";
      expect(scratchpad.get(grainType)).toEqual(["A", "B"]);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderAB21",
    async () => {
      const key = randomIntegerKey();
      await cluster.getGrain(ITestAB21FilteredGrain, key).ping();
      const grainType = "UnitTests.PlacementFilterTests.TestAB21FilteredGrain";
      expect(scratchpad.get(grainType)).toEqual(["B", "A"]);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderBA12",
    async () => {
      const key = randomIntegerKey();
      await cluster.getGrain(ITestBA12FilteredGrain, key).ping();
      const grainType = "UnitTests.PlacementFilterTests.TestBA12FilteredGrain";
      expect(scratchpad.get(grainType)).toEqual(["B", "A"]);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_OrderBA21",
    async () => {
      const key = randomIntegerKey();
      await cluster.getGrain(ITestBA21FilteredGrain, key).ping();
      const grainType = "UnitTests.PlacementFilterTests.TestBA21FilteredGrain";
      expect(scratchpad.get(grainType)).toEqual(["A", "B"]);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.GrainPlacementFilterTests.PlacementFilter_DuplicateOrder",
    async () => {
      const key = randomIntegerKey();
      await expect(
        cluster.getGrain(ITestDuplicateOrderFilteredGrain, key).ping(),
      ).rejects.toThrow();
    },
  );
});
