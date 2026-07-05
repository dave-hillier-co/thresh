// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISimpleGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface ISimpleGrain extends GrainWithIntegerKey {
  setA(a: number): Promise<void>;
  setB(b: number): Promise<void>;
  incrementA(): Promise<void>;
  getAxB(): Promise<number>;
  getAxBArgs(a: number, b: number): Promise<number>;
  getA(): Promise<number>;
}

// Upstream overloads GetAxB()/GetAxB(a, b); TS interfaces cannot overload
// across the wire (dispatch is by method name), so the two-arg form is
// getAxBArgs.
export const ISimpleGrain = defineGrainInterface<ISimpleGrain>(
  "UnitTests.GrainInterfaces.ISimpleGrain",
);
