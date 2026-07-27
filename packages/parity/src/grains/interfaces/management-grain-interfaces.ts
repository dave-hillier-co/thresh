// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidKey, GrainWithIntegerKey } from "@thresh/core/key-kinds";

/** Upstream `IDumbGrain`: a plain grain with a single, stable activation. */
export interface IDumbGrain extends GrainWithGuidKey {
  doNothing(): Promise<void>;
}

export const IDumbGrain = defineGrainInterface<IDumbGrain>("UnitTests.OrleansRuntime.IDumbGrain");

/** Upstream `IDumbWorker`: a `[StatelessWorker]` grain with no single activation. */
export interface IDumbWorker extends GrainWithIntegerKey {
  doNothing(): Promise<void>;
}

export const IDumbWorker = defineGrainInterface<IDumbWorker>(
  "UnitTests.OrleansRuntime.IDumbWorker",
);
