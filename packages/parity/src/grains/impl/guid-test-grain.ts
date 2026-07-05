// Ported from dotnet/orleans test/Grains/TestInternalGrains/TestGrain.cs (GuidTestGrain) @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { Guid } from "@tsva/core/guid";
import { IGuidTestGrain } from "@tsva/parity/grains/interfaces/guid-test-grain-interfaces";

export { IGuidTestGrain };

@grain({ name: "UnitTests.Grains.GuidTestGrain" })
export class GuidTestGrain extends Grain implements IGuidTestGrain {
  private label = "";

  override async onActivate(): Promise<void> {
    this.label = (this.id.key as Guid).toString();
  }

  async getKey(): Promise<Guid> {
    return this.id.key as Guid;
  }

  async getLabel(): Promise<string> {
    return this.label;
  }

  async setLabel(label: string): Promise<void> {
    this.label = label;
  }
}
