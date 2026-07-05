// Ported from dotnet/orleans test/Orleans.Journaling.Tests/DurableQueueTests.cs @ v10.1.0 (MIT).
//
// See durable-value-tests.test.ts for why this drives a real grain
// (DurableCollectionsGrain) through TestCluster rather than the internal
// StateMachineManager/DurableQueue surface (out of bounds for @tsva/parity),
// and why "new manager, same storage" becomes a silo restart.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@tsva/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.DurableQueueTests", () => {
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
  const freshKey = () => `queue-${nextKey++}`;

  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_BasicOperations_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.queueEnqueue("one");
      await grain.queueEnqueue("two");
      await grain.queueEnqueue("three");

      expect(await grain.queueSize()).toBe(3);

      const peeked = await grain.queuePeek();
      expect(peeked).toBe("one");
      expect(await grain.queueSize()).toBe(3);

      expect(await grain.queueDequeue()).toBe("one");
      expect(await grain.queueSize()).toBe(2);

      expect(await grain.queueDequeue()).toBe("two");
      expect(await grain.queueSize()).toBe(1);

      expect(await grain.queueDequeue()).toBe("three");
      expect(await grain.queueSize()).toBe(0);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_Persistence_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.queueEnqueue("one");
      await grain.queueEnqueue("two");
      await grain.queueEnqueue("three");

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.queueSize()).toBe(3);
      expect(await grain2.queuePeek()).toBe("one");

      const dequeued = await grain2.queueDequeue();
      expect(dequeued).toBe("one");
      expect(await grain2.queueSize()).toBe(2);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_ComplexValues_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const person1 = { id: 1, name: "John", age: 30 };
      const person2 = { id: 2, name: "Jane", age: 25 };

      await grain.queueEnqueue(person1);
      await grain.queueEnqueue(person2);

      expect(await grain.queueSize()).toBe(2);
      const peeked = (await grain.queuePeek()) as typeof person1;
      expect(peeked.name).toBe("John");

      const dequeued = (await grain.queueDequeue()) as typeof person1;
      expect(await grain.queueSize()).toBe(1);
      expect(dequeued.name).toBe("John");
      expect(dequeued.age).toBe(30);
    },
  );

  orleansTest("Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_Clear_Test", async () => {
    const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

    await grain.queueEnqueue("one");
    await grain.queueEnqueue("two");
    await grain.queueEnqueue("three");

    await grain.queueClear();

    expect(await grain.queueSize()).toBe(0);
  });

  // Upstream asserts Peek()/Dequeue() on an empty queue throw
  // InvalidOperationException. The non-throwing `peek()`/`dequeue()` stay as the
  // Try* equivalents (returning `undefined`); the throwing variants are exposed
  // through the grain as queuePeekOrThrow/queueDequeueOrThrow.
  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_EmptyQueueOperations_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      expect(await grain.queueSize()).toBe(0);

      await expect(grain.queuePeekOrThrow()).rejects.toThrow(/empty/i);
      await expect(grain.queueDequeueOrThrow()).rejects.toThrow(/empty/i);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_Enumeration_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const expectedItems = ["one", "two", "three"];
      for (const item of expectedItems) await grain.queueEnqueue(item);

      const actualItems = await grain.queueToArray();

      expect(actualItems).toEqual(expectedItems);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_LargeNumberOfOperations_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      const itemCount = 1000;
      for (let i = 0; i < itemCount; i++) await grain.queueEnqueue(i);

      expect(await grain.queueSize()).toBe(itemCount);
      expect(await grain.queuePeek()).toBe(0);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.queueSize()).toBe(itemCount);

      for (let i = 0; i < itemCount; i++) {
        expect(await grain2.queueDequeue()).toBe(i);
      }

      expect(await grain2.queueSize()).toBe(0);
    },
    30_000,
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableQueueTests.DurableQueue_Concurrent_EnqueueDequeue_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      const batchSize = 100;

      for (let i = 0; i < batchSize; i++) await grain.queueEnqueue(i);
      for (let i = 0; i < batchSize / 2; i++) await grain.queueDequeue();
      for (let i = batchSize; i < batchSize * 2; i++) await grain.queueEnqueue(i);

      expect(await grain.queueSize()).toBe(batchSize + batchSize / 2);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.queueSize()).toBe(batchSize + batchSize / 2);

      for (let i = batchSize / 2; i < batchSize; i++) {
        expect(await grain2.queueDequeue()).toBe(i);
      }
      for (let i = batchSize; i < batchSize * 2; i++) {
        expect(await grain2.queueDequeue()).toBe(i);
      }

      expect(await grain2.queueSize()).toBe(0);
    },
    15_000,
  );
});
