// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IPlacementTestGrain.cs @ v10.1.0 (MIT).
//
// Upstream `IPlacementTestGrain` also declares GetEndpoint/GetRuntimeInstanceId/
// GetActivationId/GetLocation and the load-shedding EnableOverloadDetection/
// Latch*/Unlatch* methods. A grain here has no way to learn its own hosting
// silo address (no `IGrainContext.Address` equivalent reaches `GrainRuntime`)
// and there is no overload detector/load-shedding subsystem to latch
// (GAP-PLACEMENT-INTROSPECTION, GAP-LOAD-SHEDDING), so only the
// placement-forcing members this suite's ported tests actually exercise are
// declared; callers observe placement externally via `SiloHost.isActive`.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { Guid } from "@tsva/core/guid";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";

export interface IPlacementTestGrain extends GrainWithGuidKey {
  nop(): Promise<void>;
  startPreferLocalGrain(key: Guid): Promise<Guid>;
}

export const IPlacementTestGrain = defineGrainInterface<IPlacementTestGrain>(
  "UnitTests.GrainInterfaces.IPlacementTestGrain",
);

export type IRandomPlacementTestGrain = IPlacementTestGrain;

export const IRandomPlacementTestGrain = defineGrainInterface<IRandomPlacementTestGrain>(
  "UnitTests.GrainInterfaces.IRandomPlacementTestGrain",
);

export type IPreferLocalPlacementTestGrain = IPlacementTestGrain;

export const IPreferLocalPlacementTestGrain = defineGrainInterface<IPreferLocalPlacementTestGrain>(
  "UnitTests.GrainInterfaces.IPreferLocalPlacementTestGrain",
);
