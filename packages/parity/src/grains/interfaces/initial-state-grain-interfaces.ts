// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IInitialStateGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IInitialStateGrain extends GrainKey<bigint> {
  getNames(): Promise<string[]>;
  addName(name: string): Promise<void>;
}

export const IInitialStateGrain = defineGrainInterface<IInitialStateGrain>(
  "UnitTests.GrainInterfaces.IInitialStateGrain",
);
