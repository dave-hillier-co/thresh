// Ported from dotnet/orleans test/Grains/TestInternalGrains/ClientAddressableTestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IClientAddressableTestGrain,
  type IClientAddressableTestClientObject,
} from "@thresh/parity/grains/interfaces/client-addressable-interfaces";

export { IClientAddressableTestGrain };

@grain({ name: "UnitTests.Grains.ClientAddressableTestGrain" })
export class ClientAddressableTestGrain extends Grain implements IClientAddressableTestGrain {
  private target: IClientAddressableTestClientObject | undefined;

  async setTarget(target: IClientAddressableTestClientObject): Promise<void> {
    this.target = target;
  }

  happyPath(message: string): Promise<string> {
    return this.requireTarget().onHappyPath(message);
  }

  sadPath(message: string): Promise<void> {
    return this.requireTarget().onSadPath(message);
  }

  async microSerialStressTest(iterationCount: number): Promise<void> {
    const target = this.requireTarget();
    for (let i = 0; i < iterationCount; ++i) {
      const n = await target.onSerialStress(i);
      if (n !== 10000 + i) {
        throw new Error(`microSerialStressTest: expected ${10000 + i}, got ${n}`);
      }
    }
  }

  async microParallelStressTest(iterationCount: number): Promise<void> {
    const target = this.requireTarget();
    const tasks: Array<Promise<void>> = [];
    for (let i = 0; i < iterationCount; ++i) {
      const n = i;
      tasks.push(
        target.onParallelStress(n).then((result) => {
          if (result !== 10000 + n) {
            throw new Error(`microParallelStressTest: expected ${10000 + n}, got ${result}`);
          }
        }),
      );
    }
    await Promise.all(tasks);
  }

  private requireTarget(): IClientAddressableTestClientObject {
    if (this.target === undefined) {
      throw new Error("target is not set");
    }
    return this.target;
  }
}
