// Ported from dotnet/orleans test/Grains/TestGrains/LivenessTestGrain.cs @ v10.1.0 (MIT).
// See liveness-test-grain-interfaces.ts for which methods were dropped and why.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { ILivenessTestGrain } from "@tsva/parity/grains/interfaces/liveness-test-grain-interfaces";

export { ILivenessTestGrain };

@grain({ name: "UnitTests.Grains.LivenessTestGrain" })
export class LivenessTestGrain extends Grain implements ILivenessTestGrain {
  private label = "";

  override async onActivate(): Promise<void> {
    if (this.id.key === -2n) {
      throw new Error("Primary key cannot be -2 for this test case");
    }
    this.label = (this.id.key as bigint).toString();
  }

  async getPrimaryKeyLong(): Promise<bigint> {
    return this.id.key as bigint;
  }

  async getLabel(): Promise<string> {
    return this.label;
  }

  async setLabel(label: string): Promise<void> {
    this.label = label;
  }

  async startTimer(): Promise<void> {
    this.runtime.registerTimer(async () => undefined, { seconds: 0 }, { seconds: 10 });
  }
}
