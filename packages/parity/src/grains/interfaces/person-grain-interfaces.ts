// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/EventSourcing/IPersonGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";

export type GenderType = "Male" | "Female";

export interface PersonAttributes {
  firstName: string;
  lastName: string;
  gender: GenderType;
}

// Upstream also declares `RunTentativeConfirmedStateTest` (and the grain
// implementation adds `ChangeLastName`/`ConfirmChanges`/`GetConfirmedVersion`/
// `GetTentativeVersion`), all of which exist only to probe the tentative vs.
// confirmed state split of `JournaledGrain`. This framework has no
// journaled-grain / log-consistency-provider mechanism (GAP-EVENT-SOURCING),
// so those members are omitted.
export interface IPersonGrain extends GrainWithGuidKey {
  registerBirth(person: PersonAttributes): Promise<void>;
  marry(spouse: IPersonGrain): Promise<void>;
  getTentativePersonalAttributes(): Promise<PersonAttributes>;
}

export const IPersonGrain = defineGrainInterface<IPersonGrain>(
  "UnitTests.GrainInterfaces.IPersonGrain",
);
