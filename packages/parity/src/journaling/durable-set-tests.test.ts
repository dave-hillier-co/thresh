// Ported from dotnet/orleans test/Orleans.Journaling.Tests/DurableSetTests.cs @ v10.1.0 (MIT).
//
// See durable-value-tests.test.ts for why this drives a real grain
// (DurableCollectionsGrain) through TestCluster rather than the internal
// StateMachineManager/DurableSet surface (out of bounds for @thresh/parity),
// and why "new manager, same storage" becomes a silo restart.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@thresh/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.DurableSetTests", () => {
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
  const freshKey = () => `set-${nextKey++}`;

  orleansTest(
    "Orleans.Journaling.Tests.DurableSetTests.DurableSet_BasicOperations_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const added1 = await grain.setAdd("one");
      const added2 = await grain.setAdd("two");
      const added3 = await grain.setAdd("three");
      const duplicateAdded = await grain.setAdd("one");

      expect(added1).toBe(true);
      expect(added2).toBe(true);
      expect(added3).toBe(true);
      expect(duplicateAdded).toBe(false);
      expect(await grain.setSize()).toBe(3);
      expect(await grain.setToArray()).toEqual(["one", "two", "three"]);

      const removed = await grain.setDelete("two");
      const removedNonExisting = await grain.setDelete("four");

      expect(removed).toBe(true);
      expect(removedNonExisting).toBe(false);
      expect(await grain.setSize()).toBe(2);
      expect(await grain.setToArray()).toEqual(["one", "three"]);
    },
  );

  orleansTest("Orleans.Journaling.Tests.DurableSetTests.DurableSet_Persistence_Test", async () => {
    const key = freshKey();
    const grain = cluster.getGrain(IDurableCollectionsGrain, key);

    await grain.setAdd("one");
    await grain.setAdd("two");
    await grain.setAdd("three");

    await cluster.restartSilo(cluster.primary);
    const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

    expect(await grain2.setSize()).toBe(3);
    expect(await grain2.setToArray()).toEqual(["one", "two", "three"]);
  });

  orleansTest(
    "Orleans.Journaling.Tests.DurableSetTests.DurableSet_ComplexValues_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const person1 = { Id: 1, Name: "John", Age: 30 };
      const person2 = { Id: 2, Name: "Jane", Age: 25 };
      // Same field values as person1, but a distinct instance: upstream's
      // `record class TestPerson` has value-based Equals, so this is a
      // duplicate of person1.
      const person3 = { Id: 1, Name: "John", Age: 30 };

      const added1 = await grain.setAdd(person1);
      const added2 = await grain.setAdd(person2);
      const added3 = await grain.setAdd(person3);

      expect(added1).toBe(true);
      expect(added2).toBe(true);
      expect(added3).toBe(false);
      expect(await grain.setSize()).toBe(2);

      const all = await grain.setToArray();
      expect(all).toEqual(expect.arrayContaining([person1, person2]));
      expect(all).toHaveLength(2);
    },
  );

  orleansTest("Orleans.Journaling.Tests.DurableSetTests.DurableSet_Clear_Test", async () => {
    const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

    await grain.setAdd("one");
    await grain.setAdd("two");
    await grain.setAdd("three");

    await grain.setClear();

    expect(await grain.setSize()).toBe(0);
  });

  orleansTest("Orleans.Journaling.Tests.DurableSetTests.DurableSet_Enumeration_Test", async () => {
    const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

    const expectedItems = new Set(["one", "two", "three"]);
    for (const item of expectedItems) await grain.setAdd(item);

    const actualItems = new Set(await grain.setToArray());

    expect(actualItems).toEqual(expectedItems);
  });

  orleansTest(
    "Orleans.Journaling.Tests.DurableSetTests.DurableSet_LargeNumberOfItems_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      const itemCount = 1000;
      for (let i = 0; i < itemCount; i++) await grain.setAdd(i);
      // Duplicates, which should be ignored.
      for (let i = 0; i < 100; i++) await grain.setAdd(i);

      expect(await grain.setSize()).toBe(itemCount);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.setSize()).toBe(itemCount);
      const all = new Set(await grain2.setToArray());
      for (let i = 0; i < itemCount; i++) expect(all.has(i)).toBe(true);
    },
    15_000,
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableSetTests.DurableSet_SetOperations_Test",
    async () => {
      const grain1 = cluster.getGrain(IDurableCollectionsGrain, freshKey());
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      for (let i = 0; i <= 10; i += 2) await grain1.setAdd(i);
      for (let i = 5; i <= 15; i++) await grain2.setAdd(i);

      const set1 = new Set(await grain1.setToArray());
      const set2 = new Set(await grain2.setToArray());

      const intersection = new Set([...set1].filter((v) => set2.has(v)));
      expect(intersection).toEqual(new Set([6, 8, 10]));

      const union = new Set([...set1, ...set2]);
      expect(union).toEqual(new Set([0, 2, 4, 6, 8, 10, 5, 7, 9, 11, 12, 13, 14, 15]));

      const difference = new Set([...set1].filter((v) => !set2.has(v)));
      expect(difference).toEqual(new Set([0, 2, 4]));
    },
  );

  orleansTest("Orleans.Journaling.Tests.DurableSetTests.DurableSet_ExceptWith_Test", async () => {
    const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

    for (let i = 0; i < 10; i++) await grain.setAdd(i);

    const evens: number[] = [];
    for (let i = 0; i < 10; i += 2) evens.push(i);
    for (const even of evens) await grain.setDelete(even);

    expect(await grain.setSize()).toBe(5);
    for (let i = 1; i < 10; i += 2) expect(await grain.setToArray()).toContain(i);
  });
});
