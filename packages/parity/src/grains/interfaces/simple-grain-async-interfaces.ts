// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISimpleGrainWithAsyncMethods.cs @ v10.1.0 (MIT).
// Trimmed to the members the ported SimpleGrain_AsyncMethods test exercises;
// upstream's GetX/SetX/IncrementA_Async/GetAxB_Async(a,b) aren't reached.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface ISimpleGrainWithAsyncMethods extends GrainWithIntegerKey {
  setA_Async(a: number): Promise<void>;
  setB_Async(b: number): Promise<void>;
  getAxB_Async(): Promise<number>;
}

export const ISimpleGrainWithAsyncMethods = defineGrainInterface<ISimpleGrainWithAsyncMethods>(
  "UnitTests.GrainInterfaces.ISimpleGrainWithAsyncMethods",
);
