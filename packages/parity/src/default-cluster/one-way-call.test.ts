// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/OneWayCallTests.cs @ v10.1.0 (MIT).
//
// Single-silo cluster: the client's gateway is the only silo, so there is no
// cross-silo gossip-propagation race for the observer to contend with —
// cross-silo one-way routing is covered elsewhere (see observer.test.ts).
//
// Upstream also asserts the returned Task's/ValueTask's synchronous completion
// status (`TaskStatus.RanToCompletion` / `ValueTask.IsCompleted`), which has no
// JS equivalent — there is no synchronous-completion state to observe on a
// Promise. The JS-faithful observable this port asserts instead is: a one-way
// call resolves to `undefined` without waiting on the callee, and the callee's
// side effect (the observer callback firing, and the grain's internal count)
// still lands, asynchronously, afterward.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";
import type { ClientNode } from "@thresh/client/client-node";
import { OneWayGrain } from "@thresh/parity/grains/impl/one-way-grain";
import { IOneWayGrain } from "@thresh/parity/grains/interfaces/one-way-grain-interfaces";
import { ISimpleGrainObserver } from "@thresh/parity/grains/interfaces/simple-observerable-grain-interfaces";
import { randomGuidKey } from "@thresh/parity/support/keys";
import { createClusterClient } from "@thresh/parity/support/client";

describe("DefaultCluster.Tests.General.OneWayCallTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: OneWayGrain, interfaces: [IOneWayGrain] }],
    });
    client = await createClusterClient(cluster, [
      { ctor: OneWayGrain, interfaces: [IOneWayGrain] },
    ]);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  function getGrain(): IOneWayGrain {
    return client.getGrain(IOneWayGrain, randomGuidKey());
  }

  orleansTest(
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodsReturnSynchronously_ViaClient",
    async () => {
      const grain = getGrain();
      const received: number[] = [];
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (_a: number, b: number) => {
          received.push(b);
        },
      });

      await expect(grain.notify(ref)).resolves.toBeUndefined();
      await waitFor(() => received.length >= 1);
      expect(await grain.getCount()).toBe(1);

      // This should not throw: a one-way call swallows the callee's throw.
      await expect(grain.throwsOneWay()).resolves.toBeUndefined();

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodReturnSynchronously_ViaGrain",
    async () => {
      const grain = getGrain();
      const otherGrain = getGrain();
      const received: number[] = [];
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (_a: number, b: number) => {
          received.push(b);
        },
      });

      const completedSynchronously = await grain.notifyOtherGrain(otherGrain, ref);
      expect(completedSynchronously).toBe(true);
      await waitFor(() => received.length >= 1);
      expect(await otherGrain.getCount()).toBe(1);

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodsReturnSynchronously_ViaClient_ValueTask",
    async () => {
      const grain = getGrain();
      const received: number[] = [];
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (_a: number, b: number) => {
          received.push(b);
        },
      });

      await expect(grain.notifyValueTask(ref)).resolves.toBeUndefined();
      await waitFor(() => received.length >= 1);
      expect(await grain.getCount()).toBe(1);

      // This should not throw: a one-way call swallows the callee's throw.
      await expect(grain.throwsOneWayValueTask()).resolves.toBeUndefined();

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodReturnSynchronously_ViaGrain_ValueTask",
    async () => {
      const grain = getGrain();
      const otherGrain = getGrain();
      const received: number[] = [];
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (_a: number, b: number) => {
          received.push(b);
        },
      });

      const completedSynchronously = await grain.notifyOtherGrainValueTask(otherGrain, ref);
      expect(completedSynchronously).toBe(true);
      await waitFor(() => received.length >= 1);
      expect(await otherGrain.getCount()).toBe(1);

      client.deleteObjectReference(ref);
    },
  );
});
