// Ported from dotnet/orleans test/Grains/TestGrains/SimpleGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { ISimpleGrain } from "@tsva/parity/grains/interfaces/simple-grain-interfaces";

export { ISimpleGrain };

@grain({ name: "UnitTests.Grains.SimpleGrain" })
export class SimpleGrain extends Grain implements ISimpleGrain {
  protected a = 0;
  protected b = 0;

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async setB(b: number): Promise<void> {
    this.b = b;
  }

  async incrementA(): Promise<void> {
    this.a += 1;
  }

  async getAxB(): Promise<number> {
    return this.a * this.b;
  }

  async getAxBArgs(a: number, b: number): Promise<number> {
    return a * b;
  }

  async getA(): Promise<number> {
    return this.a;
  }
}
