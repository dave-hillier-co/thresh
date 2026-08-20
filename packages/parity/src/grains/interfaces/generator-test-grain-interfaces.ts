// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IGeneratorTestGrain.cs,
// IGeneratorTestDerivedGrain1.cs, IGeneratorTestDerivedGrain2.cs,
// IGeneratorTestDerivedDerivedGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** Upstream `UnitTests.GrainInterfaces.ReturnCode`. */
export type ReturnCode = "OK" | "Fail";

/** Upstream `UnitTests.GrainInterfaces.MemberVariables`. */
export interface MemberVariables {
  byteArray: Uint8Array;
  stringVar: string;
  code: ReturnCode;
}

/** Upstream `UnitTests.GrainInterfaces.IGeneratorTestGrain`. */
export interface IGeneratorTestGrain extends GrainKey<bigint> {
  byteSet(data: Uint8Array): Promise<Uint8Array>;
  stringSet(str: string): Promise<void>;
  stringIsNullOrEmpty(): Promise<boolean>;
  getMemberVariables(): Promise<MemberVariables>;
  setMemberVariables(x: MemberVariables): Promise<void>;
}
export const IGeneratorTestGrain = defineGrainInterface<IGeneratorTestGrain>(
  "UnitTests.GrainInterfaces.IGeneratorTestGrain",
);

/** Upstream `UnitTests.GrainInterfaces.IGeneratorTestDerivedGrain1`. */
export interface IGeneratorTestDerivedGrain1 extends IGeneratorTestGrain {
  byteAppend(data: Uint8Array): Promise<Uint8Array>;
}
export const IGeneratorTestDerivedGrain1 = defineGrainInterface<IGeneratorTestDerivedGrain1>(
  "UnitTests.GrainInterfaces.IGeneratorTestDerivedGrain1",
);

/** Upstream `UnitTests.GrainInterfaces.IGeneratorTestDerivedGrain2`. */
export interface IGeneratorTestDerivedGrain2 extends IGeneratorTestGrain {
  stringConcat(str1: string, str2: string, str3: string): Promise<string>;
}
export const IGeneratorTestDerivedGrain2 = defineGrainInterface<IGeneratorTestDerivedGrain2>(
  "UnitTests.GrainInterfaces.IGeneratorTestDerivedGrain2",
);

/** Upstream `UnitTests.GrainInterfaces.ReplaceArguments`. */
export interface ReplaceArguments {
  oldString: string;
  newString: string;
}

/** Upstream `UnitTests.GrainInterfaces.IGeneratorTestDerivedDerivedGrain`. */
export interface IGeneratorTestDerivedDerivedGrain extends IGeneratorTestDerivedGrain2 {
  stringNConcat(strArray: readonly string[]): Promise<string>;
  stringReplace(strs: ReplaceArguments): Promise<string>;
}
export const IGeneratorTestDerivedDerivedGrain =
  defineGrainInterface<IGeneratorTestDerivedDerivedGrain>(
    "UnitTests.GrainInterfaces.IGeneratorTestDerivedDerivedGrain",
  );
