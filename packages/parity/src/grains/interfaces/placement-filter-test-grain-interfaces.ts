// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/GrainPlacementFilterTests.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IPingableGrain extends GrainKey<bigint> {
  ping(): Promise<void>;
}

export const ITestFilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestFilteredGrain",
);
export type ITestFilteredGrain = IPingableGrain;

/** A grain declaring no placement filters at all, sanity-checking that the filter mechanism leaves unaffected grains working. */
export const IUnfilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.IUnfilteredGrain",
);
export type IUnfilteredGrain = IPingableGrain;

export const ITestAB12FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestAB12FilteredGrain",
);
export type ITestAB12FilteredGrain = IPingableGrain;

export const ITestAB21FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestAB21FilteredGrain",
);
export type ITestAB21FilteredGrain = IPingableGrain;

export const ITestBA12FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestBA12FilteredGrain",
);
export type ITestBA12FilteredGrain = IPingableGrain;

export const ITestBA21FilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestBA21FilteredGrain",
);
export type ITestBA21FilteredGrain = IPingableGrain;

export const ITestDuplicateOrderFilteredGrain = defineGrainInterface<IPingableGrain>(
  "UnitTests.PlacementFilterTests.ITestDuplicateOrderFilteredGrain",
);
export type ITestDuplicateOrderFilteredGrain = IPingableGrain;
