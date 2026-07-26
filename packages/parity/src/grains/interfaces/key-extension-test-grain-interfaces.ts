// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IKeyExtensionTestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidCompoundKey } from "@thresh/core/key-kinds";

export interface IKeyExtensionTestGrain extends GrainWithGuidCompoundKey {
  getGrainReference(): Promise<IKeyExtensionTestGrain>;
  getActivationId(): Promise<string>;
}

export const IKeyExtensionTestGrain = defineGrainInterface<IKeyExtensionTestGrain>(
  "UnitTests.GrainInterfaces.IKeyExtensionTestGrain",
);
