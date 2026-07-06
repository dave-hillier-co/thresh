// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ObserverTests.cs @ v10.1.0 (MIT).
//
// Every test here turns on `ClientNode.createObjectReference`/
// `deleteObjectReference`: registering a plain client-side object as a
// callback target a grain can invoke through a normal grain interface
// (`ISimpleGrainObserver`). Single-silo cluster: the client's gateway is then
// the only silo, so there is no cross-silo gossip-propagation race to
// contend with — cross-silo observer routing (a grain on one silo calling
// back to a client through a different silo's gateway) is covered by
// `packages/client/src/observer-callback.cluster.test.ts`.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import type { ClientNode } from "@tsva/client/client-node";
import { SimpleObserverableGrain } from "@tsva/parity/grains/impl/simple-observerable-grain";
import {
  ISimpleGrainObserver,
  ISimpleObserverableGrain,
} from "@tsva/parity/grains/interfaces/simple-observerable-grain-interfaces";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createClusterClient } from "@tsva/parity/support/client";

describe("DefaultCluster.Tests.General.ObserverTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: SimpleObserverableGrain, interfaces: [ISimpleObserverableGrain] }],
    });
    client = await createClusterClient(cluster, [
      { ctor: SimpleObserverableGrain, interfaces: [ISimpleObserverableGrain] },
    ]);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  function getGrain(): ISimpleObserverableGrain {
    return client.getGrain(ISimpleObserverableGrain, randomIntegerKey());
  }

  orleansTest(
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SimpleNotification",
    async () => {
      const grain = getGrain();
      const seen: Array<{ a: number; b: number }> = [];
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (a: number, b: number) => {
          seen.push({ a, b });
        },
      });

      await grain.subscribe(ref);
      await grain.setA(3);
      await grain.setB(2);

      await waitFor(() => seen.some((s) => s.a === 3 && s.b === 2));

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SimpleNotification_GeneratedFactory",
    async () => {
      // Upstream distinguishes CreateObjectReference from a "generated factory"
      // path; our single createObjectReference mechanism serves both, so this
      // ports as the same observable behaviour as SimpleNotification.
      const grain = getGrain();
      const seen: Array<{ a: number; b: number }> = [];
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (a: number, b: number) => {
          seen.push({ a, b });
        },
      });

      await grain.subscribe(ref);
      await grain.setA(3);
      await grain.setB(2);

      await waitFor(() => seen.some((s) => s.a === 3 && s.b === 2));

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_DoubleSubscriptionSameReference",
    async () => {
      const grain = getGrain();
      let count = 0;
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async () => {
          count += 1;
        },
      });

      await grain.subscribe(ref);
      await grain.setA(1);
      await waitFor(() => count >= 1);

      // Subscribing the same reference again is idempotent (our ObserverManager
      // keys by reference identity), unlike upstream which rejects the second
      // subscribe with an OrleansException. The observable outcome we assert is
      // the one upstream cares about: no duplicate delivery per state change.
      await grain.subscribe(ref);
      await grain.setA(2);
      await waitFor(() => count >= 2);

      expect(count).toBe(2);

      client.deleteObjectReference(ref);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SubscribeUnsubscribe",
    async () => {
      const grain = getGrain();
      let count = 0;
      let last: { a: number; b: number } | undefined;
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async (a: number, b: number) => {
          count += 1;
          last = { a, b };
        },
      });

      await grain.subscribe(ref);
      await grain.setA(5);
      await waitFor(() => count >= 1);
      expect(last).toEqual({ a: 5, b: 0 });

      await grain.unsubscribe(ref);
      await grain.setB(3);

      // Deterministic, no wait needed: `unsubscribe` is a two-way call — by
      // the time it resolves the grain's observer set no longer contains this
      // reference, so the subsequent (awaited) setB's notify() never even
      // attempts to send to it.
      expect(count).toBe(1);

      client.deleteObjectReference(ref);
    },
  );

  orleansTest("DefaultCluster.Tests.General.ObserverTests.ObserverTest_Unsubscribe", async () => {
    const grain = getGrain();
    const ref = client.createObjectReference(ISimpleGrainObserver, {
      stateChanged: async () => undefined,
    });

    // Unsubscribing a never-subscribed observer is graceful (no throw): simply
    // awaiting it without a rejection proves that.
    await grain.unsubscribe(ref);

    client.deleteObjectReference(ref);
  });

  orleansTest(
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_DoubleSubscriptionDifferentReferences",
    async () => {
      const grain = getGrain();
      let count = 0;
      const seen: Array<{ a: number; b: number }> = [];
      const callback = async (a: number, b: number) => {
        count += 1;
        seen.push({ a, b });
      };
      const ref1 = client.createObjectReference(ISimpleGrainObserver, { stateChanged: callback });
      const ref2 = client.createObjectReference(ISimpleGrainObserver, { stateChanged: callback });

      await grain.subscribe(ref1);
      await grain.subscribe(ref2);
      await grain.setA(6);

      await waitFor(() => count >= 2);
      expect(seen).toEqual([
        { a: 6, b: 0 },
        { a: 6, b: 0 },
      ]);

      client.deleteObjectReference(ref1);
      client.deleteObjectReference(ref2);
    },
  );

  orleansTest("DefaultCluster.Tests.General.ObserverTests.ObserverTest_DeleteObject", async () => {
    const grain = getGrain();
    let count = 0;
    const ref = client.createObjectReference(ISimpleGrainObserver, {
      stateChanged: async () => {
        count += 1;
      },
    });

    await grain.subscribe(ref);
    await grain.setA(5);
    await waitFor(() => count >= 1);

    client.deleteObjectReference(ref);
    await grain.setB(3);

    // Deterministic, no wait needed: `deleteObjectReference` removes the
    // client's locally-hosted callback synchronously, so whether or not the
    // grain's one-way notify message for this setB has been sent by the time
    // it arrives, the client finds no hosted object for it and drops it
    // silently — count can never increase.
    expect(count).toBe(1);
  });

  // GAP-OBSERVERS: our surface has no distinct "raw object vs. grain reference"
  // type check at the subscribe() call site — `grain.subscribe(rawObject)`
  // would only fail once the runtime tried to serialize `rawObject` as a
  // method argument, which does not happen synchronously the way upstream's
  // `NotSupportedException` does, and our serializer's behaviour for an
  // arbitrary plain object with a `stateChanged` method is not a deliberate,
  // documented contract. Forcing an assertion here would pin an accident of
  // the current serializer rather than a real invariant.
  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SubscriberMustBeGrainReference",
  );

  orleansTest(
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_CreateObjectReference_ThrowsForInvalidArgumentTypes",
    async () => {
      const ref = client.createObjectReference(ISimpleGrainObserver, {
        stateChanged: async () => undefined,
      });

      // Attempting to create an object reference to an existing grain
      // reference throws (our surface has no separate "Grain class" concept
      // to additionally reject, unlike upstream's first assertion).
      expect(() => client.createObjectReference(ISimpleGrainObserver, ref)).toThrow(TypeError);

      client.deleteObjectReference(ref);
    },
  );
});
