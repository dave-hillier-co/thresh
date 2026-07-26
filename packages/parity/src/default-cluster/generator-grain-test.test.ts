// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/CodeGenTests/GeneratorGrainTest.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  GeneratorTestDerivedDerivedGrain,
  GeneratorTestDerivedGrain1,
  GeneratorTestDerivedGrain2,
  GeneratorTestGrain,
  IGeneratorTestDerivedDerivedGrain,
  IGeneratorTestDerivedGrain1,
  IGeneratorTestDerivedGrain2,
  IGeneratorTestGrain,
} from "@thresh/parity/grains/impl/generator-test-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

describe("Tester.CodeGenTests.GeneratorGrainTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: GeneratorTestGrain, interfaces: [IGeneratorTestGrain] },
        { ctor: GeneratorTestDerivedGrain1, interfaces: [IGeneratorTestDerivedGrain1] },
        { ctor: GeneratorTestDerivedGrain2, interfaces: [IGeneratorTestDerivedGrain2] },
        {
          ctor: GeneratorTestDerivedDerivedGrain,
          interfaces: [IGeneratorTestDerivedDerivedGrain],
        },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("Tester.CodeGenTests.GeneratorGrainTest.GeneratorGrainControlFlow", async () => {
    const grain = cluster.getGrain(IGeneratorTestGrain, randomIntegerKey());

    let isNull = await grain.stringIsNullOrEmpty();
    expect(isNull).toBe(true);

    await grain.stringSet("Begin");

    isNull = await grain.stringIsNullOrEmpty();
    expect(isNull).toBe(false);

    let members = await grain.getMemberVariables();
    expect(members.stringVar).toBe("Begin");

    const bytes = encoder.encode("ByteBegin");
    const str = "StringBegin";
    const memberVariables = { byteArray: bytes, stringVar: str, code: "Fail" as const };

    await grain.setMemberVariables(memberVariables);

    members = await grain.getMemberVariables();

    expect(decoder.decode(members.byteArray)).toBe("ByteBegin");
    expect(members.stringVar).toBe("StringBegin");
    expect(members.code).toBe("Fail");
  });

  orleansTest(
    "Tester.CodeGenTests.GeneratorGrainTest.GeneratorDerivedGrain1ControlFlow",
    async () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());

      let isNull = await grain.stringIsNullOrEmpty();
      expect(isNull).toBe(true);

      await grain.stringSet("Begin");

      isNull = await grain.stringIsNullOrEmpty();
      expect(isNull).toBe(false);

      let members = await grain.getMemberVariables();
      expect(members.stringVar).toBe("Begin");

      const bytes = encoder.encode("ByteBegin");
      const str = "StringBegin";
      const memberVariables = { byteArray: bytes, stringVar: str, code: "Fail" as const };

      await grain.setMemberVariables(memberVariables);

      members = await grain.getMemberVariables();

      expect(decoder.decode(members.byteArray)).toBe("ByteBegin");
      expect(members.stringVar).toBe("StringBegin");
      expect(members.code).toBe("Fail");
    },
  );

  orleansTest(
    "Tester.CodeGenTests.GeneratorGrainTest.GeneratorDerivedGrain2ControlFlow",
    async () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedGrain2, randomIntegerKey());

      let boolPromise = await grain.stringIsNullOrEmpty();
      expect(boolPromise).toBe(true);

      await grain.stringSet("Begin");

      boolPromise = await grain.stringIsNullOrEmpty();
      expect(boolPromise).toBe(false);

      let members = await grain.getMemberVariables();
      expect(members.stringVar).toBe("Begin");

      const bytes = encoder.encode("ByteBegin");
      const str = "StringBegin";
      const memberVariables = { byteArray: bytes, stringVar: str, code: "Fail" as const };

      await grain.setMemberVariables(memberVariables);

      members = await grain.getMemberVariables();

      expect(decoder.decode(members.byteArray)).toBe("ByteBegin");
      expect(members.stringVar).toBe("StringBegin");
      expect(members.code).toBe("Fail");

      const strPromise = await grain.stringConcat("Begin", "Cont", "End");
      expect(strPromise).toBe("BeginContEnd");
    },
  );

  orleansTest(
    "Tester.CodeGenTests.GeneratorGrainTest.GeneratorDerivedDerivedGrainControlFlow",
    async () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedDerivedGrain, randomIntegerKey());

      let isNull = await grain.stringIsNullOrEmpty();
      expect(isNull).toBe(true);

      await grain.stringSet("Begin");

      isNull = await grain.stringIsNullOrEmpty();
      expect(isNull).toBe(false);

      let members = await grain.getMemberVariables();
      expect(members.stringVar).toBe("Begin");

      const args = { oldString: "Begin", newString: "End" };
      let strPromise = await grain.stringReplace(args);
      expect(strPromise).toBe("End");

      strPromise = await grain.stringConcat("Begin", "Cont", "End");
      expect(strPromise).toBe("BeginContEnd");

      const strArray = ["Begin", "Cont", "Cont", "End"];
      strPromise = await grain.stringNConcat(strArray);
      expect(strPromise).toBe("BeginContContEnd");

      const bytes = encoder.encode("ByteBegin");
      const str = "StringBegin";
      const memberVariables = { byteArray: bytes, stringVar: str, code: "Fail" as const };

      await grain.setMemberVariables(memberVariables);

      members = await grain.getMemberVariables();

      expect(decoder.decode(members.byteArray)).toBe("ByteBegin");
      expect(members.stringVar).toBe("StringBegin");
      expect(members.code).toBe("Fail");
    },
  );

  orleansTest.excluded(
    "no assemblies in a TypeScript module system; the underlying interface-derivation " +
      "scenario is already covered by the Generator*ControlFlow tests above",
    "Tester.CodeGenTests.GeneratorGrainTest.CodeGenDerivedFromCSharpInterfaceInDifferentAssembly",
  );

  orleansTest.excluded(
    "requires runtime reflection over generic method type parameters (Type[] arguments, " +
      "closed-generic dispatch) which TypeScript's erased generics cannot express",
    "Tester.CodeGenTests.GeneratorGrainTest.GrainWithGenericMethods",
  );

  orleansTest.excluded(
    "requires runtime reflection over generic method type parameters, same as GrainWithGenericMethods",
    "Tester.CodeGenTests.GeneratorGrainTest.GenericGrainWithGenericMethods",
  );

  orleansTest.excluded(
    "combines GAP-OBSERVERS (client object references) with generic-method dispatch on the " +
      "observer interface, which TypeScript's erased generics cannot express",
    "Tester.CodeGenTests.GeneratorGrainTest.GrainObserverWithGenericMethods",
  );

  orleansTest.excluded(
    "only reachable through IGrainWithGenericMethods, which requires runtime reflection over " +
      "generic method type parameters to construct in the first place",
    "Tester.CodeGenTests.GeneratorGrainTest.GrainWithValueTaskMethod",
  );

  orleansTest.excluded(
    "F#-specific interop (derives from an F# interface in a separate assembly); no F# " +
      "language or runtime in this TypeScript framework",
    "Tester.CodeGenTests.GeneratorGrainTest.CodeGenDerivedFromFSharpInterfaceInDifferentAssembly",
  );
});
