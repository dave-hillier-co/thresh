// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ILivenessTestGrain.cs @ v10.1.0 (MIT).
// Only the methods `LivenessTests` actually asserts on are declared.
// `GetRuntimeInstanceId`/`GetUniqueId`/`GetGrainReference` are used upstream
// purely to compose a diagnostic log line (`LogGrainIdentity`) with no
// assertion attached; this framework has no silo-identity accessor exposed to
// a running grain to implement them faithfully, so they are dropped along
// with that logging-only call site (see liveness-tests.test.ts).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface ILivenessTestGrain extends GrainKey<bigint> {
  getPrimaryKeyLong(): Promise<bigint>;
  getLabel(): Promise<string>;
  setLabel(label: string): Promise<void>;
  startTimer(): Promise<void>;
}

export const ILivenessTestGrain = defineGrainInterface<ILivenessTestGrain>(
  "UnitTests.GrainInterfaces.ILivenessTestGrain",
);
