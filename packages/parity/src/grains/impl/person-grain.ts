// Ported from dotnet/orleans test/Grains/TestGrains/EventSourcing/{PersonGrain,PersonState,PersonEvents}.cs @ v10.1.0 (MIT).
// A real `JournaledGrain<PersonState, IPersonEvent>`: `registerBirth`/`marry`
// raise events and confirm before returning; `changeLastName` (used only by
// `RunTentativeConfirmedStateTest`) raises without waiting, so the tentative
// and confirmed state can diverge for a short time, matching the upstream
// test's use of that gap.
import { grain } from "@tsva/core/decorators";
import { JournaledGrain } from "@tsva/core/journaled-grain";
import {
  IPersonGrain,
  type GenderType,
  type PersonAttributes,
} from "@tsva/parity/grains/interfaces/person-grain-interfaces";
import type { Guid } from "@tsva/core/guid";

export { IPersonGrain };

interface PersonState {
  readonly firstName: string | undefined;
  readonly lastName: string | undefined;
  readonly gender: GenderType | undefined;
  readonly isMarried: boolean;
}

type PersonEvent =
  | {
      readonly kind: "registered";
      readonly firstName: string;
      readonly lastName: string;
      readonly gender: GenderType;
    }
  | {
      readonly kind: "married";
      readonly spouseId: Guid | undefined;
      readonly spouseFirstName: string;
      readonly spouseLastName: string;
    }
  | { readonly kind: "lastNameChanged"; readonly lastName: string };

function assertEqual<T>(expected: T, actual: T, what: string): void {
  if (expected !== actual) {
    throw new Error(
      `Test failed. Expected ${what} = ${String(expected)}. Actual = ${String(actual)}.`,
    );
  }
}

@grain({ name: "TestGrains.PersonGrain" })
export class PersonGrain extends JournaledGrain<PersonState, PersonEvent> implements IPersonGrain {
  initialState(): PersonState {
    return { firstName: undefined, lastName: undefined, gender: undefined, isMarried: false };
  }

  transitionState(state: PersonState, event: PersonEvent): PersonState {
    switch (event.kind) {
      case "registered":
        return {
          ...state,
          firstName: event.firstName,
          lastName: event.lastName,
          gender: event.gender,
        };
      case "married":
        return { ...state, isMarried: true };
      case "lastNameChanged":
        return { ...state, lastName: event.lastName };
    }
  }

  async registerBirth(person: PersonAttributes): Promise<void> {
    if (this.state.firstName === undefined) {
      this.raiseEvent({
        kind: "registered",
        firstName: person.firstName,
        lastName: person.lastName,
        gender: person.gender,
      });
      await this.confirmEvents();
    }
  }

  async marry(spouse: IPersonGrain): Promise<void> {
    if (this.state.isMarried) {
      throw new Error(`${this.state.lastName} is already married.`);
    }

    const spouseData = await spouse.getTentativePersonalAttributes();

    const events: PersonEvent[] = [
      // Upstream also carries `spouse.GetPrimaryKey()`; the grain reference
      // key isn't surfaced on `IPersonGrain` here, so this omits it — unused
      // by any of the ported tests/state transitions.
      {
        kind: "married",
        spouseId: undefined,
        spouseFirstName: spouseData.firstName,
        spouseLastName: spouseData.lastName,
      },
    ];
    if (this.state.lastName !== spouseData.lastName) {
      events.push({ kind: "lastNameChanged", lastName: spouseData.lastName });
    }

    this.raiseEvents(events);
    await this.confirmEvents();
  }

  private changeLastName(lastName: string): void {
    this.raiseEvent({ kind: "lastNameChanged", lastName });
    // Deliberately not awaited: the tentative and confirmed state can diverge
    // for a short time, which is exactly what `runTentativeConfirmedStateTest`
    // probes.
    void this.confirmEvents();
  }

  private confirmChanges(): Promise<void> {
    return this.confirmEvents();
  }

  async getTentativePersonalAttributes(): Promise<PersonAttributes> {
    return {
      firstName: this.tentativeState.firstName ?? "",
      lastName: this.tentativeState.lastName ?? "",
      gender: this.tentativeState.gender ?? "Male",
    };
  }

  async runTentativeConfirmedStateTest(): Promise<void> {
    assertEqual(0, this.version, "Version");
    assertEqual(0, this.tentativeVersion, "TentativeVersion");
    assertEqual(undefined, this.state.lastName, "State.LastName");
    assertEqual(undefined, this.tentativeState.lastName, "TentativeState.LastName");

    this.changeLastName("Organa");

    assertEqual(0, this.version, "Version");
    assertEqual(1, this.tentativeVersion, "TentativeVersion");
    assertEqual(undefined, this.state.lastName, "State.LastName");
    assertEqual("Organa", this.tentativeState.lastName, "TentativeState.LastName");

    await this.confirmChanges();

    assertEqual(1, this.version, "Version");
    assertEqual(1, this.tentativeVersion, "TentativeVersion");
    assertEqual("Organa", this.state.lastName, "State.LastName");
    assertEqual("Organa", this.tentativeState.lastName, "TentativeState.LastName");

    this.changeLastName("Solo");

    assertEqual(1, this.version, "Version");
    assertEqual(2, this.tentativeVersion, "TentativeVersion");
    assertEqual("Organa", this.state.lastName, "State.LastName");
    assertEqual("Solo", this.tentativeState.lastName, "TentativeState.LastName");

    for (let i = 0; i < 10 && this.version !== 2; i++) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    assertEqual(2, this.version, "Version");
    assertEqual(2, this.tentativeVersion, "TentativeVersion");
    assertEqual("Solo", this.state.lastName, "State.LastName");
    assertEqual("Solo", this.tentativeState.lastName, "TentativeState.LastName");
  }
}
