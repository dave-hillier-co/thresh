// Ported from dotnet/orleans test/Grains/TestGrains/GrainInterfaceHierarchyGrains.cs @ v10.1.0 (MIT).
// Upstream `DoIt()`/`DoMore()`/`DoThat()` return `GetType().Name`; each ported
// grain hardcodes its own class name since there is no runtime type-name
// reflection here.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IDoSomethingCombinedGrain,
  IDoSomethingEmptyGrain,
  IDoSomethingEmptyWithMoreGrain,
  IDoSomethingWithMoreEmptyGrain,
  IDoSomethingWithMoreGrain,
} from "@thresh/parity/grains/interfaces/grain-interface-hierarchy-interfaces";

export {
  IDoSomethingCombinedGrain,
  IDoSomethingEmptyGrain,
  IDoSomethingEmptyWithMoreGrain,
  IDoSomethingWithMoreEmptyGrain,
  IDoSomethingWithMoreGrain,
};

@grain({ name: "TestGrains.DoSomethingEmptyGrain" })
export class DoSomethingEmptyGrain extends Grain implements IDoSomethingEmptyGrain {
  private a = 0;

  async doIt(): Promise<string> {
    return "DoSomethingEmptyGrain";
  }

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async incrementA(): Promise<void> {
    this.a += 1;
  }

  async getA(): Promise<number> {
    return this.a;
  }
}

@grain({ name: "TestGrains.DoSomethingEmptyWithMoreGrain" })
export class DoSomethingEmptyWithMoreGrain extends Grain implements IDoSomethingEmptyWithMoreGrain {
  private a = 0;

  async doIt(): Promise<string> {
    return "DoSomethingEmptyWithMoreGrain";
  }

  async doMore(): Promise<string> {
    return "DoSomethingEmptyWithMoreGrain";
  }

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async incrementA(): Promise<void> {
    this.a += 1;
  }

  async getA(): Promise<number> {
    return this.a;
  }
}

@grain({ name: "TestGrains.DoSomethingWithMoreEmptyGrain" })
export class DoSomethingWithMoreEmptyGrain extends Grain implements IDoSomethingWithMoreEmptyGrain {
  private a = 0;

  async doIt(): Promise<string> {
    return "DoSomethingWithMoreEmptyGrain";
  }

  async doMore(): Promise<string> {
    return "DoSomethingWithMoreEmptyGrain";
  }

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async incrementA(): Promise<void> {
    this.a += 1;
  }

  async getA(): Promise<number> {
    return this.a;
  }
}

@grain({ name: "TestGrains.DoSomethingWithMoreGrain" })
export class DoSomethingWithMoreGrain extends Grain implements IDoSomethingWithMoreGrain {
  private a = 0;
  private b = 0;

  async doIt(): Promise<string> {
    return "DoSomethingWithMoreGrain";
  }

  async doThat(): Promise<string> {
    return "DoSomethingWithMoreGrain";
  }

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async incrementA(): Promise<void> {
    this.a += 1;
  }

  async getA(): Promise<number> {
    return this.a;
  }

  async setB(b: number): Promise<void> {
    this.b = b;
  }

  async incrementB(): Promise<void> {
    this.b += 1;
  }

  async getB(): Promise<number> {
    return this.b;
  }
}

@grain({ name: "TestGrains.DoSomethingCombinedGrain" })
export class DoSomethingCombinedGrain extends Grain implements IDoSomethingCombinedGrain {
  private a = 0;
  private b = 0;
  private c = 0;

  async doIt(): Promise<string> {
    return "DoSomethingCombinedGrain";
  }

  async doMore(): Promise<string> {
    return "DoSomethingCombinedGrain";
  }

  async doThat(): Promise<string> {
    return "DoSomethingCombinedGrain";
  }

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async incrementA(): Promise<void> {
    this.a += 1;
  }

  async getA(): Promise<number> {
    return this.a;
  }

  async setB(b: number): Promise<void> {
    this.b = b;
  }

  async incrementB(): Promise<void> {
    this.b += 1;
  }

  async getB(): Promise<number> {
    return this.b;
  }

  async setC(c: number): Promise<void> {
    this.c = c;
  }

  async incrementC(): Promise<void> {
    this.c += 1;
  }

  async getC(): Promise<number> {
    return this.c;
  }
}
