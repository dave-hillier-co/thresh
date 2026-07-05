// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainActivateDeactivateTests.cs @ v10.1.0 (MIT).
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import {
  ActivateDeactivateWatcherGrain,
  BadActivateDeactivateTestGrain,
  BadConstructorTestGrain,
  CreateGrainReferenceTestGrain,
  DeactivatingWhileActivatingTestGrain,
  IActivateDeactivateWatcherGrain,
  IBadActivateDeactivateTestGrain,
  IBadConstructorTestGrain,
  ICreateGrainReferenceTestGrain,
  IDeactivatingWhileActivatingTestGrain,
  ILongRunningActivateDeactivateTestGrain,
  ISimpleActivateDeactivateTestGrain,
  ITailCallActivateDeactivateTestGrain,
  ITaskActionActivateDeactivateTestGrain,
  LongRunningActivateDeactivateTestGrain,
  SimpleActivateDeactivateTestGrain,
  TailCallActivateDeactivateTestGrain,
  TaskActionActivateDeactivateTestGrain,
} from "@tsva/parity/grains/impl/activate-deactivate-test-grain";
import { ITestGrain, TestGrain } from "@tsva/parity/grains/impl/test-grain";

// See basic-activation-tests.test.ts for the full description of this defect:
// an activation whose `onActivate` throws surfaces a generic "activation
// unavailable" error to every caller, discarding the real application error.

let nextId = 1;
// Upstream: `Random.Shared.Next()`; a monotonic counter keeps activations
// distinct across tests sharing one cluster without needing a real RNG.
const freshId = (): bigint => BigInt(nextId++);

describe("DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [
        { ctor: ActivateDeactivateWatcherGrain, interfaces: [IActivateDeactivateWatcherGrain] },
        {
          ctor: SimpleActivateDeactivateTestGrain,
          interfaces: [ISimpleActivateDeactivateTestGrain],
        },
        {
          ctor: TailCallActivateDeactivateTestGrain,
          interfaces: [ITailCallActivateDeactivateTestGrain],
        },
        {
          ctor: LongRunningActivateDeactivateTestGrain,
          interfaces: [ILongRunningActivateDeactivateTestGrain],
        },
        { ctor: BadActivateDeactivateTestGrain, interfaces: [IBadActivateDeactivateTestGrain] },
        { ctor: BadConstructorTestGrain, interfaces: [IBadConstructorTestGrain] },
        {
          ctor: TaskActionActivateDeactivateTestGrain,
          interfaces: [ITaskActionActivateDeactivateTestGrain],
        },
        { ctor: CreateGrainReferenceTestGrain, interfaces: [ICreateGrainReferenceTestGrain] },
        { ctor: TestGrain, interfaces: [ITestGrain] },
        {
          ctor: DeactivatingWhileActivatingTestGrain,
          interfaces: [IDeactivatingWhileActivatingTestGrain],
        },
      ],
    });
  });

  afterAll(async () => {
    // Tearing down a 2-silo cluster after this file's many activation/
    // deactivation cycles occasionally hits an in-flight cross-silo call whose
    // peer has already stopped; that call only gives up after the runtime's
    // default 30s cross-silo call timeout (`ClusterNode.callTimeoutMs`,
    // packages/runtime/src/cluster-node.ts) rather than failing fast on
    // shutdown, so give this hook enough headroom to finish that instead of
    // spuriously failing (see bugsFound).
    await cluster.dispose();
  }, 60_000);

  const watcher = () => cluster.getGrain(IActivateDeactivateWatcherGrain, 0n);

  beforeEach(async () => {
    await watcher().clear();
  });

  afterEach(async () => {
    await watcher().clear();
  });

  async function checkNumActivateDeactivateCalls(
    expectedActivateCalls: number,
    expectedDeactivateCalls: number,
    forActivations: readonly string[],
  ): Promise<void> {
    const activateCalls = await watcher().getActivateCalls();
    expect(activateCalls).toHaveLength(expectedActivateCalls);

    const deactivateCalls = await watcher().getDeactivateCalls();
    expect(deactivateCalls).toHaveLength(expectedDeactivateCalls);

    for (let i = 0; i < expectedActivateCalls; i += 1) {
      expect(activateCalls[i]).toBe(forActivations[i]);
    }
    for (let i = 0; i < expectedDeactivateCalls; i += 1) {
      expect(deactivateCalls[i]).toBe(forActivations[i]);
    }
  }

  const flush = () => new Promise((r) => setTimeout(r, 0));

  /** Deactivate `grain` via its own `doDeactivate()` and let the fake-time collector sweep it. */
  async function deactivate(grain: { doDeactivate(): Promise<void> }): Promise<void> {
    await grain.doDeactivate();
    time.advance(60_000);
    await flush();
  }

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.WatcherGrain_GetGrain",
    async () => {
      const grain = cluster.getGrain(IActivateDeactivateWatcherGrain, 1n);
      await grain.clear();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Activate_Simple",
    async () => {
      const grain = cluster.getGrain(ISimpleActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await checkNumActivateDeactivateCalls(1, 0, [activation]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Deactivate_Simple",
    async () => {
      const grain = cluster.getGrain(ISimpleActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await deactivate(grain);
      await checkNumActivateDeactivateCalls(1, 1, [activation]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Reactivate_Simple",
    async () => {
      const grain = cluster.getGrain(ISimpleActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await deactivate(grain);
      await checkNumActivateDeactivateCalls(1, 1, [activation]);

      const activation2 = await grain.doSomething();
      expect(activation2).not.toBe(activation);
      await checkNumActivateDeactivateCalls(2, 1, [activation, activation2]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Activate_TailCall",
    async () => {
      const grain = cluster.getGrain(ITailCallActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await checkNumActivateDeactivateCalls(1, 0, [activation]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Deactivate_TailCall",
    async () => {
      const grain = cluster.getGrain(ITailCallActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await deactivate(grain);
      await checkNumActivateDeactivateCalls(1, 1, [activation]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Reactivate_TailCall",
    async () => {
      const grain = cluster.getGrain(ITailCallActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await deactivate(grain);
      await checkNumActivateDeactivateCalls(1, 1, [activation]);

      const activation2 = await grain.doSomething();
      expect(activation2).not.toBe(activation);
      await checkNumActivateDeactivateCalls(2, 1, [activation, activation2]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.LongRunning_Deactivate",
    async () => {
      const grain = cluster.getGrain(ILongRunningActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await checkNumActivateDeactivateCalls(1, 0, [activation]);

      await deactivate(grain);
      await checkNumActivateDeactivateCalls(1, 1, [activation]);

      const activation2 = await grain.doSomething();
      expect(activation2).not.toBe(activation);
      await checkNumActivateDeactivateCalls(2, 1, [activation, activation2]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.BadActivate_Await",
    async () => {
      const grain = cluster.getGrain(IBadActivateDeactivateTestGrain, freshId());
      try {
        await grain.throwSomething();
        expect.fail("Expected ThrowSomething call to fail as unable to Activate grain");
      } catch (exc) {
        expect((exc as Error).message).toContain("Application-OnActivateAsync");
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.BadActivate_GetValue",
    async () => {
      const grain = cluster.getGrain(IBadActivateDeactivateTestGrain, freshId());
      try {
        const key = await grain.getKey();
        expect.fail(
          "Expected ThrowSomething call to fail as unable to Activate grain, but returned " +
            String(key),
        );
      } catch (exc) {
        expect((exc as Error).message).toContain("Application-OnActivateAsync");
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.BadActivate_Await_ViaOtherGrain",
    async () => {
      const id = freshId();
      const grain = cluster.getGrain(ICreateGrainReferenceTestGrain, id);
      try {
        await grain.forwardCall(cluster.getGrain(IBadActivateDeactivateTestGrain, id));
        expect.fail("Expected ThrowSomething call to fail as unable to Activate grain");
      } catch (exc) {
        expect((exc as Error).message).toContain("Application-OnActivateAsync");
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Constructor_Bad_Await",
    async () => {
      const grain = cluster.getGrain(IBadConstructorTestGrain, freshId());
      try {
        await grain.doSomething();
        expect.fail("Expected ThrowSomething call to fail as unable to Activate grain");
      } catch (exc) {
        const message = (exc as Error).message;
        expect(exc).not.toBeInstanceOf(TypeError);
        expect(message).toContain("Constructor");
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.Constructor_CreateGrainReference",
    async () => {
      const grain = cluster.getGrain(ICreateGrainReferenceTestGrain, freshId());
      const activation = await grain.doSomething();
      expect(activation).toBeTruthy();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.TaskAction_Deactivate",
    async () => {
      const grain = cluster.getGrain(ITaskActionActivateDeactivateTestGrain, freshId());
      const activation = await grain.doSomething();
      await deactivate(grain);
      await checkNumActivateDeactivateCalls(1, 1, [activation]);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.ActivationsLifeCycleTests.GrainActivateDeactivateTests.DeactivateOnIdleWhileActivate",
    async () => {
      const grain = cluster.getGrain(IDeactivatingWhileActivatingTestGrain, freshId());
      await expect(grain.doSomething()).rejects.toThrow("activation unavailable");
    },
  );
});
