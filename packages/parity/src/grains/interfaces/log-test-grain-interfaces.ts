// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ILogTestGrain.cs @ v10.1.0 (MIT).
// Upstream declares a much larger surface (reservations, conditional writes,
// raw event-log reads, etc.) used across many log-consistency-provider test
// suites. Only the members `LogTestGrainClearTests` needs are ported here.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface AB {
  a: number;
  b: number;
}

export interface ILogTestGrain extends GrainKey<bigint> {
  getAGlobal(): Promise<number>;
  getALocal(): Promise<number>;
  getBothGlobal(): Promise<AB>;
  getBothLocal(): Promise<AB>;
  getConfirmedVersion(): Promise<number>;
  setAGlobal(a: number): Promise<void>;
  setALocal(a: number): Promise<void>;
  incrementALocal(): Promise<void>;
  incrementAGlobal(): Promise<void>;
  setBGlobal(b: number): Promise<void>;
  setBLocal(b: number): Promise<void>;
  clear(): Promise<void>;
}

export const ILogTestGrain = defineGrainInterface<ILogTestGrain>(
  "UnitTests.GrainInterfaces.ILogTestGrain",
);
