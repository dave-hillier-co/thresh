// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/INullStateGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface NullableState {
  name: string | null;
}

export interface INullStateGrain extends GrainWithIntegerKey {
  setStateAndDeactivate(state: NullableState | null): Promise<void>;
  getState(): Promise<NullableState | null>;
}

export const INullStateGrain = defineGrainInterface<INullStateGrain>(
  "UnitTests.GrainInterfaces.INullStateGrain",
);
