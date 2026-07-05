// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StartupTaskTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { ISimpleGrain, SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";

describe("DefaultCluster.Tests.StartupTaskTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }],
      configureSilo: (builder) => {
        // Mirrors upstream's inline-function startup task.
        builder.addStartupTask(async (grains) => {
          const grain = grains.getGrain(ISimpleGrain, 1n);
          await grain.setA(888);
        });
        // Mirrors upstream's class-based startup task (CallGrainStartupTask).
        builder.addStartupTask(async (grains) => {
          const grain = grains.getGrain(ISimpleGrain, 2n);
          await grain.setA(777);
        });
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.StartupTaskTests.StartupTaskCanCallGrains", async () => {
    const grain1 = cluster.getGrain(ISimpleGrain, 1n);
    const grain2 = cluster.getGrain(ISimpleGrain, 2n);

    expect(await grain1.getA()).toBe(888);
    expect(await grain2.getA()).toBe(777);
  });
});
