// Ported from dotnet/orleans test/Grains/TestGrains/KeyExtensionTestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { CompoundKey } from "@tsva/core/grain-key";
import { Guid } from "@tsva/core/guid";
import { IKeyExtensionTestGrain } from "@tsva/parity/grains/interfaces/key-extension-test-grain-interfaces";

export { IKeyExtensionTestGrain };

@grain({ name: "UnitTests.Grains.KeyExtensionTestGrain" })
export class KeyExtensionTestGrain extends Grain implements IKeyExtensionTestGrain {
  private readonly uniqueId = Guid.newGuid();

  async getGrainReference(): Promise<IKeyExtensionTestGrain> {
    return this.getGrain(IKeyExtensionTestGrain, this.id.key as CompoundKey<Guid>);
  }

  async getActivationId(): Promise<string> {
    return this.uniqueId.toString();
  }
}
