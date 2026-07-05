// Ported from dotnet/orleans test/Grains/TestGrains/AsyncSimpleGrain.cs @ v10.1.0 (MIT).
// Upstream inserts a 1s `Task.Delay` before every async method resolves purely to
// exercise the async/await path; shortened to a few ms so the ported test doesn't
// sleep for seconds (no timing assertion depends on the exact delay).
import { grain } from "@tsva/core/decorators";
import { SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { ISimpleGrainWithAsyncMethods } from "@tsva/parity/grains/interfaces/simple-grain-async-interfaces";

export { ISimpleGrainWithAsyncMethods };

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

@grain({ name: "UnitTests.Grains.AsyncSimpleGrain" })
export class AsyncSimpleGrain extends SimpleGrain implements ISimpleGrainWithAsyncMethods {
  async setA_Async(a: number): Promise<void> {
    await delay(5);
    await this.setA(a);
  }

  async setB_Async(b: number): Promise<void> {
    await delay(5);
    await this.setB(b);
  }

  async getAxB_Async(): Promise<number> {
    await delay(5);
    return this.getAxB();
  }
}
