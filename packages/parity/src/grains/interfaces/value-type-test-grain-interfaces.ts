// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IGenericInterfaces.cs
// (`IValueTypeTestGrain`, `ValueTypeTestData`) @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";

/** Upstream `ValueTypeTestData`: a plain value (C# struct), ported as data. */
export interface ValueTypeTestData {
  value: number;
}

export interface IValueTypeTestGrain extends GrainWithGuidKey {
  getStateData(): Promise<ValueTypeTestData>;
  setStateData(d: ValueTypeTestData): Promise<void>;
}

export const IValueTypeTestGrain = defineGrainInterface<IValueTypeTestGrain>(
  "UnitTests.GrainInterfaces.IValueTypeTestGrain",
);
