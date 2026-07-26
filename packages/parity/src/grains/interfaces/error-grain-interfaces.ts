// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IErrorGrain.cs @ v10.1.0 (MIT).
// Trimmed to the members the ported DefaultCluster error-handling tests exercise;
// upstream's LogMessage/SetAError/SetBError/GetAxBError(a,b)/Dispose/Unobserved*
// aren't reached by any ported test.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { ISimpleGrain } from "@thresh/parity/grains/interfaces/simple-grain-interfaces";

export interface IErrorGrain extends ISimpleGrain {
  getAxBError(): Promise<number>;
  longMethodWithError(waitTimeMs: number): Promise<void>;
  delayMethod(milliseconds: number): Promise<void>;
  addChildren(children: IErrorGrain[]): Promise<void>;
}

export const IErrorGrain = defineGrainInterface<IErrorGrain>(
  "UnitTests.GrainInterfaces.IErrorGrain",
);
