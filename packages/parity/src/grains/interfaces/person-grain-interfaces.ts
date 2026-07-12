// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/EventSourcing/IPersonGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";

export type GenderType = "Male" | "Female";

export interface PersonAttributes {
  firstName: string;
  lastName: string;
  gender: GenderType;
}

export interface IPersonGrain extends GrainWithGuidKey {
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
