// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainLevelCallFilterTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  IMethodInterceptionGrain,
  MethodInterceptionGrain,
} from "@tsva/parity/grains/impl/method-interception-grain";

describe("UnitTests.General.GrainLevelCallFilterTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [{ ctor: MethodInterceptionGrain, interfaces: [IMethodInterceptionGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.General.GrainLevelCallFilterTests.GrainCallFilter_Incoming_GrainLevel_Without_Global_Filter_Test",
    async () => {
      const grain = cluster.getGrain(IMethodInterceptionGrain, 0n);

      expect(await grain.one()).toBe("intercepted one with no args");

      // Grain interceptors should receive the MethodInfo of the implementation, not the interface.
      expect(await grain.echo("stao erom tae")).toBe("eat more oats");

      expect(await grain.notIntercepted()).toBe("not intercepted");
      expect(await grain.sayHello()).toBe("Hello");
    },
  );
});
