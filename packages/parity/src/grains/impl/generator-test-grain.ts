// Ported from dotnet/orleans test/Grains/TestGrains/GeneratorTestGrain.cs,
// GeneratorTestDerivedGrain1.cs, GeneratorTestDerivedGrain2.cs,
// GeneratorTestDerivedDerivedGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IGeneratorTestDerivedDerivedGrain,
  IGeneratorTestDerivedGrain1,
  IGeneratorTestDerivedGrain2,
  IGeneratorTestGrain,
  type MemberVariables,
  type ReplaceArguments,
  type ReturnCode,
} from "@thresh/parity/grains/interfaces/generator-test-grain-interfaces";

export {
  IGeneratorTestDerivedDerivedGrain,
  IGeneratorTestDerivedGrain1,
  IGeneratorTestDerivedGrain2,
  IGeneratorTestGrain,
};

@grain({ name: "UnitTests.Grains.GeneratorTestGrain" })
export class GeneratorTestGrain extends Grain implements IGeneratorTestGrain {
  protected myGrainBytes: Uint8Array = new Uint8Array(0);
  protected myGrainString = "";
  protected myCode: ReturnCode = "OK";

  async byteSet(data: Uint8Array): Promise<Uint8Array> {
    this.myGrainBytes = data.slice();
    return this.myGrainBytes;
  }

  async stringSet(str: string): Promise<void> {
    this.myGrainString = str;
  }

  async stringIsNullOrEmpty(): Promise<boolean> {
    return this.myGrainString.length === 0;
  }

  async getMemberVariables(): Promise<MemberVariables> {
    return { byteArray: this.myGrainBytes, stringVar: this.myGrainString, code: this.myCode };
  }

  async setMemberVariables(x: MemberVariables): Promise<void> {
    this.myGrainBytes = x.byteArray.slice();
    this.myGrainString = x.stringVar;
    this.myCode = x.code;
  }
}

@grain({ name: "UnitTests.Grains.GeneratorTestDerivedGrain1" })
export class GeneratorTestDerivedGrain1
  extends GeneratorTestGrain
  implements IGeneratorTestDerivedGrain1
{
  async byteAppend(data: Uint8Array): Promise<Uint8Array> {
    const combined = new Uint8Array(this.myGrainBytes.length + data.length);
    combined.set(this.myGrainBytes, 0);
    combined.set(data, this.myGrainBytes.length);
    this.myGrainBytes = combined;
    return this.myGrainBytes;
  }
}

@grain({ name: "UnitTests.Grains.GeneratorTestDerivedGrain2" })
export class GeneratorTestDerivedGrain2
  extends GeneratorTestGrain
  implements IGeneratorTestDerivedGrain2
{
  async stringConcat(str1: string, str2: string, str3: string): Promise<string> {
    return str1 + str2 + str3;
  }
}

@grain({ name: "UnitTests.Grains.GeneratorTestDerivedDerivedGrain" })
export class GeneratorTestDerivedDerivedGrain
  extends GeneratorTestDerivedGrain2
  implements IGeneratorTestDerivedDerivedGrain
{
  async stringNConcat(strArray: readonly string[]): Promise<string> {
    return strArray.join("");
  }

  async stringReplace(strs: ReplaceArguments): Promise<string> {
    this.myGrainString = this.myGrainString.split(strs.oldString).join(strs.newString);
    return this.myGrainString;
  }
}
