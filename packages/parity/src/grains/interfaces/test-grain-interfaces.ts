// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ITestGrain.cs @ v10.1.0 (MIT).
// Upstream also declares GetRuntimeInstanceId/GetActivationId/GetGrainReference/
// StartTimer/TestRequestContext/DoLongAction; only the subset the ported
// BasicActivationTests cases exercise is declared here.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface ITestGrain extends GrainWithIntegerKey {
  getKey(): Promise<bigint>;
  getLabel(): Promise<string>;
  setLabel(label: string): Promise<void>;
  getMultipleGrainInterfaces_Array(): Promise<unknown[]>;
  getMultipleGrainInterfaces_List(): Promise<unknown[]>;
}

export const ITestGrain = defineGrainInterface<ITestGrain>("UnitTests.GrainInterfaces.ITestGrain");

/** Upstream's `ITestGrainLongOnActivateAsync`: activation validates the key like `ITestGrain`. */
export interface ITestGrainLongOnActivateAsync extends GrainWithIntegerKey {
  getKey(): Promise<bigint>;
}

export const ITestGrainLongOnActivateAsync = defineGrainInterface<ITestGrainLongOnActivateAsync>(
  "UnitTests.GrainInterfaces.ITestGrainLongOnActivateAsync",
);
