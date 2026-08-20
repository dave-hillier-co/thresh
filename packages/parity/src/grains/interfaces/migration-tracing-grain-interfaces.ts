// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";

/** A migration-capable grain with NO migration participant at all (see `ISimpleMigrationTracingTestGrain`). */
export interface ISimpleMigrationTracingGrain extends GrainKey<bigint> {
  setState(state: number): Promise<void>;
  getState(): Promise<number>;
  migrateOnIdle(target?: SiloAddress): Promise<void>;
}

export const ISimpleMigrationTracingGrain = defineGrainInterface<ISimpleMigrationTracingGrain>(
  "UnitTests.GrainInterfaces.ISimpleMigrationTracingGrain",
);

/**
 * A migration-capable grain that ALSO declares a placement filter (see
 * `IMigrationFilterTracingTestGrain`) — backs
 * `MigrationPlacementFilterSpanIsParentedUnderPlaceGrainSpan`, which verifies
 * a `FilterPlacementCandidates` span is created and parented under `PlaceGrain`
 * when migration triggers placement.
 */
export interface IMigrationFilterTracingGrain extends GrainKey<bigint> {
  setState(state: number): Promise<void>;
  getState(): Promise<number>;
  migrateOnIdle(target?: SiloAddress): Promise<void>;
}

export const IMigrationFilterTracingGrain = defineGrainInterface<IMigrationFilterTracingGrain>(
  "UnitTests.GrainInterfaces.IMigrationFilterTracingGrain",
);
