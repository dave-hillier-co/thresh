// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Guid } from "@thresh/core/guid";

/** Upstream `IDumbGrain`: a plain grain with a single, stable activation. */
export interface IDumbGrain extends GrainKey<Guid> {
  doNothing(): Promise<void>;
}

export const IDumbGrain = defineGrainInterface<IDumbGrain>("UnitTests.OrleansRuntime.IDumbGrain");

/** Upstream `IDumbWorker`: a `[StatelessWorker]` grain with no single activation. */
export interface IDumbWorker extends GrainKey<bigint> {
  doNothing(): Promise<void>;
}

export const IDumbWorker = defineGrainInterface<IDumbWorker>(
  "UnitTests.OrleansRuntime.IDumbWorker",
);
