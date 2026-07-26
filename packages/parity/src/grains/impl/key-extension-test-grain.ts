// Ported from dotnet/orleans test/Grains/TestGrains/KeyExtensionTestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { CompoundKey } from "@thresh/core/grain-key";
import { Guid } from "@thresh/core/guid";
import { IKeyExtensionTestGrain } from "@thresh/parity/grains/interfaces/key-extension-test-grain-interfaces";

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
