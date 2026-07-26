// Ported from dotnet/orleans test/Grains/TestInternalGrains/TestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  ITestGrain,
  ITestGrainLongOnActivateAsync,
} from "@thresh/parity/grains/interfaces/test-grain-interfaces";

export { ITestGrain, ITestGrainLongOnActivateAsync };

@grain({ name: "UnitTests.Grains.TestGrain" })
export class TestGrain extends Grain implements ITestGrain {
  private label = "";

  override async onActivate(): Promise<void> {
    const key = this.id.key as bigint;
    // Key values of -2 are not allowed in this test case.
    if (key === -2n) throw new Error("Primary key cannot be -2 for this test case");
    this.label = key.toString();
  }

  async getKey(): Promise<bigint> {
    return this.id.key as bigint;
  }

  async getLabel(): Promise<string> {
    return this.label;
  }

  async setLabel(label: string): Promise<void> {
    this.label = label;
  }

  async getMultipleGrainInterfaces_Array(): Promise<unknown[]> {
    const grains: unknown[] = [];
    for (let i = 0; i < 5; i += 1) grains.push(this.getGrain(ITestGrain, BigInt(i)));
    return grains;
  }

  async getMultipleGrainInterfaces_List(): Promise<unknown[]> {
    return this.getMultipleGrainInterfaces_Array();
  }

  async testRequestContext(): Promise<[string | undefined, string | undefined]> {
    this.runtime.setRequestContext("jarjar", "binks");

    const bar1 = new Promise<string | undefined>((resolve) => {
      setImmediate(() => resolve(this.runtime.getRequestContext("jarjar")));
    });
    const bar2 = new Promise<string | undefined>((resolve) => {
      setImmediate(() => resolve(this.runtime.getRequestContext("jarjar")));
    });

    return Promise.all([bar1, bar2]);
  }
}

@grain({ name: "UnitTests.Grains.TestGrainLongOnActivateAsync" })
export class TestGrainLongOnActivateAsync extends Grain implements ITestGrainLongOnActivateAsync {
  override async onActivate(): Promise<void> {
    const key = this.id.key as bigint;
    if (key === -2n) throw new Error("Primary key cannot be -2 for this test case");
  }

  async getKey(): Promise<bigint> {
    return this.id.key as bigint;
  }
}
