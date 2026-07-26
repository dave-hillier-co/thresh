// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainInterfaceHierarchyTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  DoSomethingCombinedGrain,
  DoSomethingEmptyGrain,
  DoSomethingEmptyWithMoreGrain,
  DoSomethingWithMoreEmptyGrain,
  DoSomethingWithMoreGrain,
  IDoSomethingCombinedGrain,
  IDoSomethingEmptyGrain,
  IDoSomethingEmptyWithMoreGrain,
  IDoSomethingWithMoreEmptyGrain,
  IDoSomethingWithMoreGrain,
} from "@thresh/parity/grains/impl/grain-interface-hierarchy-grains";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("DefaultCluster.Tests.GrainInterfaceHierarchyTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: DoSomethingEmptyGrain, interfaces: [IDoSomethingEmptyGrain] },
        { ctor: DoSomethingEmptyWithMoreGrain, interfaces: [IDoSomethingEmptyWithMoreGrain] },
        { ctor: DoSomethingWithMoreEmptyGrain, interfaces: [IDoSomethingWithMoreEmptyGrain] },
        { ctor: DoSomethingWithMoreGrain, interfaces: [IDoSomethingWithMoreGrain] },
        { ctor: DoSomethingCombinedGrain, interfaces: [IDoSomethingCombinedGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.GrainInterfaceHierarchyTests.DoSomethingGrainEmptyTest",
    async () => {
      const doSomething = cluster.getGrain(IDoSomethingEmptyGrain, randomIntegerKey());
      expect(await doSomething.doIt()).toBe("DoSomethingEmptyGrain");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainInterfaceHierarchyTests.DoSomethingGrainEmptyWithMoreTest",
    async () => {
      const doSomething = cluster.getGrain(IDoSomethingEmptyWithMoreGrain, randomIntegerKey());
      expect(await doSomething.doIt()).toBe("DoSomethingEmptyWithMoreGrain");
      expect(await doSomething.doMore()).toBe("DoSomethingEmptyWithMoreGrain");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainInterfaceHierarchyTests.DoSomethingWithMoreEmptyGrainTest",
    async () => {
      const doSomething = cluster.getGrain(IDoSomethingWithMoreEmptyGrain, randomIntegerKey());
      expect(await doSomething.doIt()).toBe("DoSomethingWithMoreEmptyGrain");
      expect(await doSomething.doMore()).toBe("DoSomethingWithMoreEmptyGrain");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainInterfaceHierarchyTests.DoSomethingWithMoreGrainTest",
    async () => {
      const doSomething = cluster.getGrain(IDoSomethingWithMoreGrain, randomIntegerKey());
      expect(await doSomething.doIt()).toBe("DoSomethingWithMoreGrain");
      expect(await doSomething.doThat()).toBe("DoSomethingWithMoreGrain");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainInterfaceHierarchyTests.DoSomethingCombinedGrainTest",
    async () => {
      const doSomething = cluster.getGrain(IDoSomethingCombinedGrain, randomIntegerKey());
      expect(await doSomething.doIt()).toBe("DoSomethingCombinedGrain");
      expect(await doSomething.doMore()).toBe("DoSomethingCombinedGrain");
      expect(await doSomething.doThat()).toBe("DoSomethingCombinedGrain");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainInterfaceHierarchyTests.DoSomethingValidateSingleGrainTest",
    async () => {
      const doSomethingEmptyGrain = cluster.getGrain(IDoSomethingEmptyGrain, randomIntegerKey());
      const doSomethingEmptyWithMoreGrain = cluster.getGrain(
        IDoSomethingEmptyWithMoreGrain,
        randomIntegerKey(),
      );
      const doSomethingWithMoreEmptyGrain = cluster.getGrain(
        IDoSomethingWithMoreEmptyGrain,
        randomIntegerKey(),
      );
      const doSomethingWithMoreGrain = cluster.getGrain(
        IDoSomethingWithMoreGrain,
        randomIntegerKey(),
      );
      const doSomethingCombinedGrain = cluster.getGrain(
        IDoSomethingCombinedGrain,
        randomIntegerKey(),
      );

      await doSomethingEmptyGrain.setA(10);
      await doSomethingEmptyWithMoreGrain.setA(10);
      await doSomethingWithMoreEmptyGrain.setA(10);
      await doSomethingWithMoreGrain.setA(10);
      await doSomethingWithMoreGrain.setB(10);
      await doSomethingCombinedGrain.setA(10);
      await doSomethingCombinedGrain.setB(10);
      await doSomethingCombinedGrain.setC(10);

      await doSomethingEmptyGrain.incrementA();
      await doSomethingEmptyWithMoreGrain.incrementA();
      await doSomethingWithMoreEmptyGrain.incrementA();
      await doSomethingWithMoreGrain.incrementA();
      await doSomethingWithMoreGrain.incrementB();
      await doSomethingCombinedGrain.incrementA();
      await doSomethingCombinedGrain.incrementB();
      await doSomethingCombinedGrain.incrementC();

      expect(await doSomethingEmptyGrain.getA()).toBe(11);
      expect(await doSomethingEmptyWithMoreGrain.getA()).toBe(11);
      expect(await doSomethingWithMoreEmptyGrain.getA()).toBe(11);
      expect(await doSomethingWithMoreGrain.getA()).toBe(11);
      expect(await doSomethingWithMoreGrain.getB()).toBe(11);
      expect(await doSomethingCombinedGrain.getA()).toBe(11);
      expect(await doSomethingCombinedGrain.getB()).toBe(11);
      expect(await doSomethingCombinedGrain.getC()).toBe(11);
    },
  );
});
