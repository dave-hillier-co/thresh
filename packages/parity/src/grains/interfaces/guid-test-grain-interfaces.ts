// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ITestGrain.cs (IGuidTestGrain) @ v10.1.0 (MIT).
// Upstream also declares GetRuntimeInstanceId/GetActivationId/GetSiloAddress;
// only the subset the ported BasicActivationTests case exercises is declared here.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { Guid } from "@thresh/core/guid";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";

export interface IGuidTestGrain extends GrainWithGuidKey {
  getKey(): Promise<Guid>;
  getLabel(): Promise<string>;
  setLabel(label: string): Promise<void>;
}

export const IGuidTestGrain = defineGrainInterface<IGuidTestGrain>(
  "UnitTests.GrainInterfaces.IGuidTestGrain",
);
