// Ported from dotnet/orleans test/Orleans.Journaling.Tests/LogSegmentTests.cs @ v10.1.0 (MIT).
//
// Upstream's abstract LogSegmentTests runs its DurableList suite twice, once
// per IStateMachineStorageProvider backend: AzureStorageLogSegmentTests and
// InMemoryLogSegmentTests. Only the in-memory backend applies here (Azure
// Blob storage is an external, .NET-specific storage provider — see
// docs/deviations.md). See durable-value-tests.test.ts for why this drives a
// real grain (DurableCollectionsGrain) through TestCluster rather than the
// internal StateMachineManager/DurableList surface, and why "new manager,
// same storage" becomes a silo restart.
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@tsva/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.AzureStorageLogSegmentTests", () => {
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_BasicOperations_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_Persistence_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_ComplexValues_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_Clear_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_Contains_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_InsertAndRemove_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_Enumeration_Test",
  );
  orleansTest.excluded(
    "Azure Blob Storage is an external, .NET-specific journal storage provider",
    "Orleans.Journaling.Tests.AzureStorageLogSegmentTests.DurableList_LargeNumberOfOperations_And_Snapshot_Test",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // orleansTest.excluded above, so this placeholder keeps the suite valid.
  it.skip("(all tests in this suite are orleansTest.excluded — see above)", () => undefined);
});

describe("Orleans.Journaling.Tests.InMemoryLogSegmentTests", () => {
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
  const freshKey = () => `logseg-${nextKey++}`;

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_BasicOperations_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.listAdd("one");
      await grain.listAdd("two");
      await grain.listAdd("three");

      expect(await grain.listLength()).toBe(3);
      expect(await grain.listGet(0)).toBe("one");
      expect(await grain.listGet(1)).toBe("two");
      expect(await grain.listGet(2)).toBe("three");

      await grain.listSet(1, "updated");
      expect(await grain.listGet(1)).toBe("updated");

      await grain.listRemoveAt(0);
      expect(await grain.listLength()).toBe(2);
      expect(await grain.listGet(0)).toBe("updated");
      expect(await grain.listGet(1)).toBe("three");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_Persistence_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.listAdd("one");
      await grain.listAdd("two");
      await grain.listAdd("three");

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.listLength()).toBe(3);
      expect(await grain2.listGet(0)).toBe("one");
      expect(await grain2.listGet(1)).toBe("two");
      expect(await grain2.listGet(2)).toBe("three");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_ComplexValues_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      const person1 = { id: 1, name: "John", age: 30 };
      const person2 = { id: 2, name: "Jane", age: 25 };

      await grain.listAdd(person1);
      await grain.listAdd(person2);

      expect(await grain.listLength()).toBe(2);
      expect(((await grain.listGet(0)) as typeof person1).name).toBe("John");
      expect(((await grain.listGet(1)) as typeof person2).age).toBe(25);

      // `list[0] = list[0] with { Age = 31 }` — a fresh copy with the updated
      // field, re-set at the same index (rather than the mutate-in-place
      // pattern used elsewhere), so it round-trips through persistence.
      const current = (await grain.listGet(0)) as typeof person1;
      await grain.listSet(0, { ...current, age: 31 });

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);
      expect(((await grain2.listGet(0)) as typeof person1).age).toBe(31);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_Clear_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.listAdd("one");
      await grain.listAdd("two");
      await grain.listAdd("three");

      await grain.listClear();

      expect(await grain.listLength()).toBe(0);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);
      expect(await grain2.listLength()).toBe(0);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_Contains_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.listAdd("one");
      await grain.listAdd("two");
      await grain.listAdd("three");

      expect(await grain.listContains("two")).toBe(true);
      expect(await grain.listContains("four")).toBe(false);

      await grain.listRemove("two");

      expect(await grain.listContains("two")).toBe(false);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_InsertAndRemove_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.listAdd("one");
      await grain.listAdd("three");

      await grain.listInsert(1, "two");

      expect(await grain.listLength()).toBe(3);
      expect(await grain.listGet(0)).toBe("one");
      expect(await grain.listGet(1)).toBe("two");
      expect(await grain.listGet(2)).toBe("three");

      const removed = await grain.listRemove("two");

      expect(removed).toBe(true);
      expect(await grain.listLength()).toBe(2);
      expect(await grain.listGet(0)).toBe("one");
      expect(await grain.listGet(1)).toBe("three");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_Enumeration_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const expectedItems = ["one", "two", "three"];
      for (const item of expectedItems) await grain.listAdd(item);

      const actualItems = await grain.listToArray();

      expect(actualItems).toEqual(expectedItems);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.InMemoryLogSegmentTests.DurableList_LargeNumberOfOperations_And_Snapshot_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      const itemCount = 100;
      for (let i = 0; i < itemCount; i++) await grain.listAdd(i);

      expect(await grain.listLength()).toBe(itemCount);

      for (let i = 0; i < itemCount; i += 2) {
        await grain.listSet(i, ((await grain.listGet(i)) as number) * 2);
      }

      for (let i = 0; i < itemCount; i++) {
        if (i % 2 === 0) expect(await grain.listGet(i)).toBe(i * 2);
        else expect(await grain.listGet(i)).toBe(i);
      }

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.listLength()).toBe(itemCount);
      for (let i = 0; i < itemCount; i++) {
        if (i % 2 === 0) expect(await grain2.listGet(i)).toBe(i * 2);
        else expect(await grain2.listGet(i)).toBe(i);
      }
    },
    15_000,
  );
});
