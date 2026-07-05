// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IInitialStateGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IInitialStateGrain extends GrainWithIntegerKey {
  getNames(): Promise<string[]>;
  addName(name: string): Promise<void>;
}

export const IInitialStateGrain = defineGrainInterface<IInitialStateGrain>(
  "UnitTests.GrainInterfaces.IInitialStateGrain",
);
