// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IExternalTypeGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** Upstream `UnitTests.GrainInterfaces.EnumClass`. */
export interface EnumClass {
  enumsList: readonly string[];
}

/**
 * Upstream `UnitTests.GrainInterfaces.IExternalTypeGrain`.
 *
 * Upstream also declares `GetAbstractModel(IEnumerable<NameObjectCollectionBase>)`,
 * exercising serialization of a legacy .NET collection type external to the
 * grain assembly; no test calls it and there is no TS analog for
 * `NameObjectCollectionBase`, so it is omitted here.
 */
export interface IExternalTypeGrain extends GrainKey<bigint> {
  getEnumModel(): Promise<EnumClass>;
}
export const IExternalTypeGrain = defineGrainInterface<IExternalTypeGrain>(
  "UnitTests.GrainInterfaces.IExternalTypeGrain",
);
