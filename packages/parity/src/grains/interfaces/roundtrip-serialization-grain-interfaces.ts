// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IValueTypeTestGrain.cs
// (`IRoundtripSerializationGrain`, `ParamVal`, `RetVal`, `CampaignEnemyTestType`) @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

/** Upstream `CampaignEnemyTestType` enum, ported as its member names. */
export type CampaignEnemyTestType = "None" | "Brute" | "Enemy1" | "Enemy2" | "Enemy3";

/** Upstream `ParamVal` record. */
export interface ParamVal {
  value: number;
}

/** Upstream `RetVal` record. */
export interface RetVal {
  value: number;
}

/** Upstream `UnitTests.GrainInterfaces.IRoundtripSerializationGrain`. */
export interface IRoundtripSerializationGrain extends GrainWithIntegerKey {
  getEnemyType(): Promise<CampaignEnemyTestType>;
  getClosedGenericValue(): Promise<unknown>;
  getRetValForParamVal(param: ParamVal): Promise<RetVal>;
}
export const IRoundtripSerializationGrain = defineGrainInterface<IRoundtripSerializationGrain>(
  "UnitTests.GrainInterfaces.IRoundtripSerializationGrain",
);
