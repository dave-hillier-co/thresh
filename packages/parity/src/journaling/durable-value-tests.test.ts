// Ported from dotnet/orleans test/Orleans.Journaling.Tests/DurableValueTests.cs @ v10.1.0 (MIT).
//
// Upstream builds a bare StateMachineManager + DurableValue<T> in-process and
// calls WriteStateAsync explicitly; that internal surface lives in
// @thresh/journaling, which packages/parity cannot depend on (workspace
// boundary). Instead this drives a real grain (DurableCollectionsGrain,
// @durableState-backed) through TestCluster: each mutator already persists
// synchronously (no separate flush step), and "recreate the manager from the
// same storage" becomes "restart the (only) silo and fetch the grain again" —
// the cluster's journal storage is shared cluster-wide, exactly like the
// upstream test's shared `sut.Storage`.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@thresh/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.DurableValueTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: DurableCollectionsGrain, interfaces: [IDurableCollectionsGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  let nextKey = 0;
  const freshKey = () => `value-${nextKey++}`;

  orleansTest(
    "Orleans.Journaling.Tests.DurableValueTests.DurableValue_BasicOperations_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.setValue("Hello World");
      expect(await grain.getValue()).toBe("Hello World");

      await grain.setValue("Updated Value");
      expect(await grain.getValue()).toBe("Updated Value");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableValueTests.DurableValue_Persistence_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.setValue(42);

      // Recreate the manager from the same storage: restart the silo, then
      // fetch a fresh activation of the same grain key.
      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.getValue()).toBe(42);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableValueTests.DurableValue_NullValue_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.setValue(null);
      expect(await grain.getValue()).toBeNull();

      await grain.setValue("Not null anymore");
      expect(await grain.getValue()).toBe("Not null anymore");

      await grain.setValue(null);
      expect(await grain.getValue()).toBeNull();
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableValueTests.DurableValue_ComplexType_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const person = { id: 1, name: "John Doe", age: 30 };
      await grain.setValue(person);

      const stored = (await grain.getValue()) as typeof person;
      expect(stored).not.toBeNull();
      expect(stored.id).toBe(1);
      expect(stored.name).toBe("John Doe");
      expect(stored.age).toBe(30);

      // In-process transport passes objects by reference (no serialization
      // boundary), so mutating the returned object mutates the grain's live
      // in-memory value directly, matching upstream's `durableValue.Value.Age = 31`
      // without a further explicit Set call.
      stored.age = 31;
      expect(((await grain.getValue()) as typeof person).age).toBe(31);
    },
  );
});
