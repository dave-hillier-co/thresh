// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IPlacementTestGrain.cs @ v10.1.0 (MIT).
//
// Upstream `IPlacementTestGrain` also declares GetEndpoint/GetActivationId/
// GetLocation and the load-shedding EnableOverloadDetection/Latch*/Unlatch*
// methods. There is no overload detector/load-shedding subsystem to latch
// (GAP-LOAD-SHEDDING), so only the placement-forcing members and
// `GetRuntimeInstanceId` (a grain reporting its own hosting silo, via
// `GrainRuntime.localSiloAddress()`) that this suite's ported tests actually
// exercise are declared.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { Guid } from "@tsva/core/guid";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";

export interface IPlacementTestGrain extends GrainWithGuidKey {
  nop(): Promise<void>;
  startPreferLocalGrain(key: Guid): Promise<Guid>;
  /** Upstream `GetRuntimeInstanceId()`: this activation's hosting silo identity. */
  getRuntimeInstanceId(): Promise<string>;
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
