// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IKeyExtensionTestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { CompoundKey } from "@thresh/core/grain-key";
import type { Guid } from "@thresh/core/guid";

export interface IKeyExtensionTestGrain extends GrainKey<CompoundKey<Guid>> {
  getGrainReference(): Promise<IKeyExtensionTestGrain>;
  getActivationId(): Promise<string>;
}

export const IKeyExtensionTestGrain = defineGrainInterface<IKeyExtensionTestGrain>(
  "UnitTests.GrainInterfaces.IKeyExtensionTestGrain",
);
