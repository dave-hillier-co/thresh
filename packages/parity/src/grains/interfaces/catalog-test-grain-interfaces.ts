// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ICatalogTestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface ICatalogTestGrain extends GrainWithIntegerKey {
  initialize(): Promise<void>;
  blastCallNewGrains(nGrains: number, startingKey: bigint, nCallsToEach: number): Promise<void>;
}

export const ICatalogTestGrain = defineGrainInterface<ICatalogTestGrain>(
  "UnitTests.GrainInterfaces.ICatalogTestGrain",
);
