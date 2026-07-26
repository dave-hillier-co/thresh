// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/SimpleGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { ISimpleGrain, SimpleGrain } from "@thresh/parity/grains/impl/simple-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("DefaultCluster.Tests.General.SimpleGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  const getSimpleGrain = () => cluster.getGrain(ISimpleGrain, randomIntegerKey());

  orleansTest("DefaultCluster.Tests.General.SimpleGrainTests.SimpleGrainGetGrain", async () => {
    const grain = getSimpleGrain();
    await grain.getAxB();
  });

  orleansTest("DefaultCluster.Tests.General.SimpleGrainTests.SimpleGrainControlFlow", async () => {
    const grain = getSimpleGrain();

    await grain.setA(2);
    await grain.setB(3);

    expect(await grain.getAxB()).toBe(6);
  });

  orleansTest("DefaultCluster.Tests.General.SimpleGrainTests.SimpleGrainDataFlow", async () => {
    const grain = getSimpleGrain();

    const setAPromise = grain.setA(3);
    const setBPromise = grain.setB(4);
    await Promise.all([setAPromise, setBPromise]);

    expect(await grain.getAxB()).toBe(12);
  });

  orleansTest.excluded(
    "skipped upstream: grains with multiple constructors are unsupported without explicit registration",
    "DefaultCluster.Tests.General.SimpleGrainTests.GettingGrainWithMultipleConstructorsActivesViaDefaultConstructor",
  );
});
