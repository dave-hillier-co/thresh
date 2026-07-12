// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/EchoTaskGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { GrainCallTimeoutError } from "@tsva/core/errors";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
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

  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_Local",
    async () => {
      const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
      await grain.pingLocalSiloAsync();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_Remote",
    async () => {
      const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
      const [silo1, silo2] = cluster.silos;
      if (silo1 === undefined || silo2 === undefined) {
        throw new Error("expected at least two silos in the cluster");
      }
      await grain.pingRemoteSiloAsync(silo1.address);
      await grain.pingRemoteSiloAsync(silo2.address);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_OtherSilo",
    async () => {
      const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
      await grain.pingOtherSiloAsync();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_PingSilo_OtherSilo_Membership",
    async () => {
      const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
      await grain.pingClusterMemberAsync();
    },
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

// Upstream drives these with a real 5s [ResponseTimeout] and Thread.Sleep,
// asserting the elapsed time falls within bounds. Here a FakeTimeProvider
// makes the deadline race deterministic: the caller's promise rejects with
// GrainCallTimeoutError the instant the fake clock crosses the 5s deadline,
// while the callee's turn is left running (never interrupted). All three
// upstream variants (ContinueWith/.Result/async-await) collapse to the same
// await/rejects assertion shape in JS, which has no equivalent of a
// synchronous blocking `.Result` wait.
describe("DefaultCluster.Tests.General.EchoTaskGrainTests.Timeout", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  // Single silo: the response deadline is enforced caller-side, so a second
  // silo adds nothing but a teardown race.
  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 1,
      time,
      grains: [{ ctor: EchoTaskGrain, interfaces: [IEchoTaskGrain] }],
    });
  });

  // Safety net: advance well past any callee timer and drain so no suspended
  // turn lingers into dispose, where deactivation disposes the callee's timer
  // and would wedge the still-suspended turn behind the exclusive deactivate turn.
  afterAll(async () => {
    time.advance(60_000);
    await flush();
    await cluster.dispose();
  });

  const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

  // A blocking call whose 6s delay outlasts its 5s [ResponseTimeout]: crossing
  // the 5s deadline rejects the caller with GrainCallTimeoutError while the
  // callee's turn keeps running (never interrupted). Then advance past the 6s
  // callee timer and drain so the turn completes and does not linger into the
  // next test / teardown.
  async function expectResponseDeadline(): Promise<void> {
    const grain = cluster.getGrain(IEchoTaskGrain, randomGuidKey());
    const promise = grain.blockingCallTimeoutAsync(6);
    const assertion = expect(promise).rejects.toBeInstanceOf(GrainCallTimeoutError);
    // The dispatch turn arms the caller-side deadline timer, and the callee turn
    // arms its 6s timer, as microtasks — drain them before advancing so both are
    // registered when the fake clock crosses the 5s deadline.
    await flush();
    time.advance(5_000);
    await assertion;
    time.advance(2_000);
    await flush();
  }

  // All three upstream variants (ContinueWith / async-await / .Result) collapse
  // to the same await/rejects assertion in JS, which has no synchronous .Result.
  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Timeout_ContinueWith",
    expectResponseDeadline,
  );
  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Timeout_Await",
    expectResponseDeadline,
  );
  orleansTest(
    "DefaultCluster.Tests.General.EchoTaskGrainTests.EchoGrain_Timeout_Result",
    expectResponseDeadline,
  );
});
