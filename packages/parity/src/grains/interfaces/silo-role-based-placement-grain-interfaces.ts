// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISiloRoleBasedPlacementGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface ISiloRoleBasedPlacementGrain extends GrainKey<string> {
  ping(): Promise<boolean>;
}

export const ISiloRoleBasedPlacementGrain = defineGrainInterface<ISiloRoleBasedPlacementGrain>(
  "UnitTests.GrainInterfaces.ISiloRoleBasedPlacementGrain",
);
