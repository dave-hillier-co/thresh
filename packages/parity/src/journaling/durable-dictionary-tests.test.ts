// Ported from dotnet/orleans test/Orleans.Journaling.Tests/DurableDictionaryTests.cs @ v10.1.0 (MIT).
//
// See durable-value-tests.test.ts for why this drives a real grain
// (DurableCollectionsGrain) through TestCluster rather than the internal
// StateMachineManager/DurableDictionary surface (out of bounds for
// @tsva/parity), and why "new manager, same storage" becomes a silo restart.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@tsva/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.DurableDictionaryTests", () => {
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
  const freshKey = () => `dict-${nextKey++}`;

  orleansTest(
    "Orleans.Journaling.Tests.DurableDictionaryTests.DurableDictionary_BasicOperations_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.dictSet("one", 1);
      await grain.dictSet("two", 2);
      await grain.dictSet("three", 3);

      expect(await grain.dictSize()).toBe(3);
      expect(await grain.dictGet("one")).toBe(1);
      expect(await grain.dictGet("two")).toBe(2);
      expect(await grain.dictGet("three")).toBe(3);

      await grain.dictSet("two", 22);
      expect(await grain.dictGet("two")).toBe(22);

      const removed = await grain.dictDelete("three");
      expect(removed).toBe(true);
      expect(await grain.dictSize()).toBe(2);
      expect(await grain.dictHas("three")).toBe(false);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableDictionaryTests.DurableDictionary_Persistence_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.dictSet("one", 1);
      await grain.dictSet("two", 2);
      await grain.dictSet("three", 3);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.dictSize()).toBe(3);
      expect(await grain2.dictGet("one")).toBe(1);
      expect(await grain2.dictGet("two")).toBe(2);
      expect(await grain2.dictGet("three")).toBe(3);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableDictionaryTests.DurableDictionary_ComplexKeys_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const key1 = { id: 1, name: "Key1" };
      const key2 = { id: 2, name: "Key2" };

      await grain.dictSet(key1, "Value1");
      await grain.dictSet(key2, "Value2");

      expect(await grain.dictSize()).toBe(2);
      // Same object references passed back for lookup: the in-process
      // transport passes objects by reference, so this is the same identity
      // the dictionary stored the entry under.
      expect(await grain.dictGet(key1)).toBe("Value1");
      expect(await grain.dictGet(key2)).toBe("Value2");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableDictionaryTests.DurableDictionary_ComplexValues_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const person1 = { id: 1, name: "John", age: 30 };
      const person2 = { id: 2, name: "Jane", age: 25 };

      await grain.dictSet("person1", person1);
      await grain.dictSet("person2", person2);

      expect(await grain.dictSize()).toBe(2);
      expect(((await grain.dictGet("person1")) as typeof person1).name).toBe("John");
      expect(((await grain.dictGet("person2")) as typeof person2).age).toBe(25);

      // Mutate the returned (shared-reference) object in place, matching
      // upstream's `dictionary["person1"].Age = 31` without a further Set call.
      const stored = (await grain.dictGet("person1")) as typeof person1;
      stored.age = 31;
      expect(((await grain.dictGet("person1")) as typeof person1).age).toBe(31);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableDictionaryTests.DurableDictionary_Clear_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.dictSet("one", 1);
      await grain.dictSet("two", 2);
      await grain.dictSet("three", 3);

      await grain.dictClear();

      expect(await grain.dictSize()).toBe(0);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableDictionaryTests.DurableDictionary_Enumeration_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const expectedPairs: Record<string, number> = { one: 1, two: 2, three: 3 };
      for (const [k, v] of Object.entries(expectedPairs)) await grain.dictSet(k, v);

      const actualEntries = await grain.dictEntries();
      const actualPairs = Object.fromEntries(actualEntries as [string, number][]);
      expect(actualPairs).toEqual(expectedPairs);

      expect((await grain.dictKeys()).sort()).toEqual(Object.keys(expectedPairs).sort());
      expect((await grain.dictValues()).sort()).toEqual(Object.values(expectedPairs).sort());
    },
  );
});
