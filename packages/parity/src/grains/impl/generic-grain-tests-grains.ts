// Ported from dotnet/orleans test/Grains/TestGrains/GenericGrains.cs @ v10.1.0 (MIT)
// (GrainWithNoProperties, GrainWithListFields only — see
// generic-grain-tests-interfaces.ts).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IGrainWithListFields,
  IGrainWithNoProperties,
} from "@thresh/parity/grains/interfaces/generic-grain-tests-interfaces";

export { IGrainWithListFields, IGrainWithNoProperties };

@grain({ name: "UnitTests.Grains.GrainWithNoProperties" })
export class GrainWithNoProperties extends Grain implements IGrainWithNoProperties {
  async getAxB(a: number, b: number): Promise<string> {
    return `${a}x${b}`;
  }
}

@grain({ name: "UnitTests.Grains.GrainWithListFields" })
export class GrainWithListFields extends Grain implements IGrainWithListFields {
  private items: string[] = [];

  async addItem(item: string): Promise<void> {
    this.items.push(item);
  }

  async getItems(): Promise<readonly string[]> {
    return this.items;
  }
}
