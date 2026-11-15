// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/GrainPlacementFilterTests.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IPingableGrain extends GrainKey<bigint> {
  ping(): Promise<void>;
}

export const ITestFilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestFilteredGrain",
  { key: "integer" },
);
export type ITestFilteredGrain = IPingableGrain;

/** A grain declaring no placement filters at all, sanity-checking that the filter mechanism leaves unaffected grains working. */
export const IUnfilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.IUnfilteredGrain",
  { key: "integer" },
);
export type IUnfilteredGrain = IPingableGrain;

export const ITestAB12FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestAB12FilteredGrain",
  { key: "integer" },
);
export type ITestAB12FilteredGrain = IPingableGrain;

export const ITestAB21FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestAB21FilteredGrain",
  { key: "integer" },
);
export type ITestAB21FilteredGrain = IPingableGrain;

export const ITestBA12FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestBA12FilteredGrain",
  { key: "integer" },
);
export type ITestBA12FilteredGrain = IPingableGrain;

export const ITestBA21FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestBA21FilteredGrain",
  { key: "integer" },
);
export type ITestBA21FilteredGrain = IPingableGrain;

export const ITestDuplicateOrderFilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestDuplicateOrderFilteredGrain",
  { key: "integer" },
);
export type ITestDuplicateOrderFilteredGrain = IPingableGrain;
