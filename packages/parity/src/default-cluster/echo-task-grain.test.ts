// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/EchoTaskGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  BlockingEchoTaskGrain,
  EchoGrain,
  EchoTaskGrain,
  IBlockingEchoTaskGrain,
  IEchoGrain,
  IEchoTaskGrain,
  IReentrantBlockingEchoTaskGrain,
  ReentrantBlockingEchoTaskGrain,
} from "@tsva/parity/grains/impl/echo-task-grain";
import { randomGuidKey, randomIntegerKey } from "@tsva/parity/support/keys";

const expectedEcho = "Hello from EchoGrain";
const expectedEchoError = "Error from EchoGrain";

describe("DefaultCluster.Tests.General.EchoTaskGrainTests", () => {
  let cluster: TestCluster;

  // Upstream runs against a cluster with at least one secondary silo.
  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: EchoGrain, interfaces: [IEchoGrain] },
        { ctor: EchoTaskGrain, interfaces: [IEchoTaskGrain] },
        { ctor: BlockingEchoTaskGrain, interfaces: [IBlockingEchoTaskGrain] },
        { ctor: ReentrantBlockingEchoTaskGrain, interfaces: [IReentrantBlockingEchoTaskGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_GetGrain", () => {
    // Getting a reference is a local operation that must not touch the cluster.
    cluster.getGrain(IEchoTaskGrain, randomGuidKey());
  });

  orleansTest("DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Echo", async () => {
    const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
    expect(await grain.echoAsync(expectedEcho)).toBe(expectedEcho);
  });

  orleansTest("DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_EchoError", async () => {
    const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
    await expect(grain.echoErrorAsync(expectedEchoError)).rejects.toThrow(expectedEchoError);
  });

  orleansTest.gap(
    "GAP-CANCELLATION",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Timeout_ContinueWith",
  );

  orleansTest.gap(
    "GAP-CANCELLATION",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Timeout_Await",
  );

  orleansTest.gap(
    "GAP-CANCELLATION",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Timeout_Result",
  );

  orleansTest("DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_LastEcho", async () => {
    const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());

    expect(await grain.echoAsync(expectedEcho)).toBe(expectedEcho);
    expect(await grain.getLastEchoAsync()).toBe(expectedEcho);

    await expect(grain.echoErrorAsync(expectedEchoError)).rejects.toThrow(expectedEchoError);
    expect(await grain.getLastEchoAsync()).toBe(expectedEchoError);
  });

  orleansTest("DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Ping", async () => {
    const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
    await grain.pingAsync();
  });

  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_Local",
  );

  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_Remote",
  );

  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_OtherSilo",
  );

  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_OtherSilo_Membership",
  );

  orleansTest("DefaultCluster.Tests.General.EchoTaskGrainTests.EchoTaskGrain_Await", async () => {
    const grain = cluster.getGrain(IBlockingEchoTaskGrain, randomIntegerKey());

    expect(await grain.echo(expectedEcho)).toBe(expectedEcho);
    expect(await grain.callMethodAV_Await(expectedEcho)).toBe(expectedEcho);
    expect(await grain.callMethodTask_Await(expectedEcho)).toBe(expectedEcho);
  });

  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoTaskGrain_Await_Reentrant",
    async () => {
      const grain = cluster.getGrain(IReentrantBlockingEchoTaskGrain, randomIntegerKey());

      expect(await grain.echo(expectedEcho)).toBe(expectedEcho);
      expect(await grain.callMethodAV_Await(expectedEcho)).toBe(expectedEcho);
      expect(await grain.callMethodTask_Await(expectedEcho)).toBe(expectedEcho);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_EchoNullable",
    async () => {
      const grain = cluster.getGrain(IEchoGrain, randomGuidKey());

      const now = new Date();
      const received = await grain.echoNullable(now);
      expect(received?.getTime()).toBe(now.getTime());

      expect(await grain.echoNullable(null)).toBeNull();
    },
  );
});
