// Ported from dotnet/orleans test/Grains/TestGrains/ExternalTypeGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { EnumClass } from "@thresh/parity/grains/interfaces/external-type-grain-interfaces";
import { IExternalTypeGrain } from "@thresh/parity/grains/interfaces/external-type-grain-interfaces";

export { IExternalTypeGrain };

@grain({ name: "UnitTests.Grains.ExternalTypeGrain" })
export class ExternalTypeGrain extends Grain implements IExternalTypeGrain {
  async getEnumModel(): Promise<EnumClass> {
    return { enumsList: ["Local"] };
  }
}
