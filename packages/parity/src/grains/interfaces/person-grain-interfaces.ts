// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/EventSourcing/IPersonGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Guid } from "@thresh/core/guid";

export type GenderType = "Male" | "Female";

export interface PersonAttributes {
  firstName: string;
  lastName: string;
  gender: GenderType;
}

export interface IPersonGrain extends GrainKey<Guid> {
  registerBirth(person: PersonAttributes): Promise<void>;
  marry(spouse: IPersonGrain): Promise<void>;
  getTentativePersonalAttributes(): Promise<PersonAttributes>;
  /**
   * Upstream test-only member: probes the tentative-vs-confirmed state split
   * from inside the grain, since the interleaving of its steps has to be
   * deterministic (see `PersonGrainTests.JournaledGrainTests_TentativeConfirmedState`).
   */
  runTentativeConfirmedStateTest(): Promise<void>;
}

export const IPersonGrain = defineGrainInterface<IPersonGrain>(
  "UnitTests.GrainInterfaces.IPersonGrain",
);
