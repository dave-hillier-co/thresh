// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISiloRoleBasedPlacementGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

export interface ISiloRoleBasedPlacementGrain extends GrainWithStringKey {
  ping(): Promise<boolean>;
}

export const ISiloRoleBasedPlacementGrain = defineGrainInterface<ISiloRoleBasedPlacementGrain>(
  "UnitTests.GrainInterfaces.ISiloRoleBasedPlacementGrain",
);
