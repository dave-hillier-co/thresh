// Ported from dotnet/orleans test/Grains/TestGrains/EventSourcing/PersonGrain.cs @ v10.1.0 (MIT).
// Upstream is a `JournaledGrain<PersonState, IPersonEvent>`; `RegisterBirth`
// and `Marry` raise events and `ConfirmEvents()` before returning, and
// `GetTentativePersonalAttributes` reads `TentativeState`. This framework has
// no journaled-grain machinery (GAP-EVENT-SOURCING); the ported tests here
// only observe state after each call completes (no concurrent races), so
// plain in-memory fields reproduce the same outcomes.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import {
  IPersonGrain,
  type PersonAttributes,
} from "@tsva/parity/grains/interfaces/person-grain-interfaces";

export { IPersonGrain };

@grain({ name: "TestGrains.PersonGrain" })
export class PersonGrain extends Grain implements IPersonGrain {
  private attributes: PersonAttributes | undefined;
  private married = false;

  async registerBirth(person: PersonAttributes): Promise<void> {
    if (this.attributes === undefined) {
      this.attributes = { ...person };
    }
  }

  async marry(spouse: IPersonGrain): Promise<void> {
    if (this.married) {
      throw new Error(`${this.attributes?.lastName} is already married.`);
    }

    const spouseData = await spouse.getTentativePersonalAttributes();

    this.married = true;
    if (this.attributes !== undefined && this.attributes.lastName !== spouseData.lastName) {
      this.attributes = { ...this.attributes, lastName: spouseData.lastName };
    }
  }

  async getTentativePersonalAttributes(): Promise<PersonAttributes> {
    if (this.attributes === undefined) {
      return { firstName: "", lastName: "", gender: "Male" };
    }
    return { ...this.attributes };
  }
}
