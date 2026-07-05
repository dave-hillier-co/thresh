// Ported from dotnet/orleans test/Grains/TestGrains/ExternalTypeGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { EnumClass } from "@tsva/parity/grains/interfaces/external-type-grain-interfaces";
import { IExternalTypeGrain } from "@tsva/parity/grains/interfaces/external-type-grain-interfaces";

export { IExternalTypeGrain };

@grain({ name: "UnitTests.Grains.ExternalTypeGrain" })
export class ExternalTypeGrain extends Grain implements IExternalTypeGrain {
  async getEnumModel(): Promise<EnumClass> {
    return { enumsList: ["Local"] };
  }
}
