// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISimpleDIGrain.cs @ v10.1.0 (MIT),
// trimmed to the members GrainActivatorTests exercises.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface ISimpleDiGrain extends GrainWithIntegerKey {
  getStringValue(): Promise<string>;
  getLongValue(): Promise<bigint>;
  doDeactivate(): Promise<void>;
}

export const ISimpleDiGrain = defineGrainInterface<ISimpleDiGrain>(
  "UnitTests.GrainInterfaces.ISimpleDIGrain",
);
