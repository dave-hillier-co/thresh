// Ported from dotnet/orleans test/Grains/TestGrains/PromiseForwardGrain.cs @ v10.1.0 (MIT).
// Trimmed to the members ISimpleGrain declares that the ported test reaches
// (getAxBArgs, forwarded to a fresh SimpleGrain instance).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { ISimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { IPromiseForwardGrain } from "@tsva/parity/grains/interfaces/promise-forward-grain-interfaces";
import { randomIntegerKey } from "@tsva/parity/support/keys";

export { IPromiseForwardGrain };

@grain({ name: "UnitTests.Grains.PromiseForwardGrain" })
export class PromiseForwardGrain extends Grain implements IPromiseForwardGrain {
  private mySimpleGrain: ISimpleGrain | undefined;

  async setA(a: number): Promise<void> {
    await this.getSimpleGrain().setA(a);
  }

  async setB(b: number): Promise<void> {
    await this.getSimpleGrain().setB(b);
  }

  async incrementA(): Promise<void> {
    await this.getSimpleGrain().incrementA();
  }

  async getAxB(): Promise<number> {
    return this.getSimpleGrain().getAxB();
  }

  async getAxBArgs(a: number, b: number): Promise<number> {
    return this.getSimpleGrain().getAxBArgs(a, b);
  }

  async getA(): Promise<number> {
    return this.getSimpleGrain().getA();
  }

  private getSimpleGrain(): ISimpleGrain {
    this.mySimpleGrain ??= this.getGrain(ISimpleGrain, randomIntegerKey());
    return this.mySimpleGrain;
  }
}
