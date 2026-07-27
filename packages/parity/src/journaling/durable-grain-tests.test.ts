// Ported from dotnet/orleans test/Orleans.Journaling.Tests/DurableGrainTests.cs @ v10.1.0 (MIT).
//
// Every test here forces a fresh grain activation via
// `grain.Cast<IGrainManagementExtension>().DeactivateOnIdle()`, then asserts
// `GetActivationId()` changed before re-checking durable state. This framework's
// equivalent (GAP-GRAIN-EXTENSION, now closed) is
// `castGrainReference(grain, IGrainManagementExtension).deactivateOnIdle()` —
// the built-in, auto-installed extension every activation gets (see
// `@thresh/runtime/grain-management-extension`) — with `DurableCollectionsGrain`'s
// `activationTag()` standing in for Orleans' `GetActivationId()` (a per-
// activation random tag rather than a real ActivationId, but the same "did a
// fresh activation run" signal the upstream tests rely on).
//
// Upstream drives four distinct grain interfaces (ITestDurableGrain,
// ITestDurableGrainWithComplexState, ITestMultiCollectionGrain,
// ITestDurableGrainInterface); this framework maps all of them onto the one
// composite `DurableCollectionsGrain` already used by the rest of the
// journaling parity suite (durable-value-tests.test.ts and friends), using its
// `value` for simple/complex single-value state and its `dict`/`list`/`queue`/
// `set` for the multi-collection cases.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { castGrainReference } from "@thresh/core/grain-reference";
import { IGrainManagementExtension } from "@thresh/runtime/grain-management-extension";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@thresh/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.DurableGrainTests", () => {
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
  const freshKey = () => `grain-${nextKey++}`;

  /** Force a fresh activation of `target` and assert the activation actually changed. */
  async function deactivateOnIdleAndAssertFresh(target: IDurableCollectionsGrain): Promise<void> {
    const tagBefore = await target.activationTag();
    const ext = castGrainReference(target, IGrainManagementExtension);
    await ext.deactivateOnIdle();
    const tagAfter = await target.activationTag();
    expect(tagAfter).not.toBe(tagBefore);
  }

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_State_Persistence_Test",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await target.setValue({ name: "Test Name", counter: 42 });

      expect(await target.getValue()).toEqual({ name: "Test Name", counter: 42 });

      await deactivateOnIdleAndAssertFresh(target);

      expect(await target.getValue()).toEqual({ name: "Test Name", counter: 42 });
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_Update_State_Test",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await target.setValue({ name: "Initial Name", counter: 10 });
      await target.setValue({ name: "Updated Name", counter: 20 });

      expect(await target.getValue()).toEqual({ name: "Updated Name", counter: 20 });

      await deactivateOnIdleAndAssertFresh(target);

      expect(await target.getValue()).toEqual({ name: "Updated Name", counter: 20 });
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_Complex_Types_Test",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const person = { id: 1, name: "John Doe", age: 30 };
      const items = ["Item1", "Item2", "Item3"];
      await target.setValue(person);
      for (const item of items) await target.listAdd(item);

      expect(await target.getValue()).toEqual(person);
      expect(await target.listLength()).toBe(3);

      await deactivateOnIdleAndAssertFresh(target);

      const retrievedPerson = await target.getValue();
      const retrievedItems = await target.listToArray();

      expect(retrievedPerson).toEqual({ id: 1, name: "John Doe", age: 30 });
      expect(retrievedItems).toEqual(["Item1", "Item2", "Item3"]);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_Multiple_Collections_Test",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await target.dictSet("key1", 1);
      await target.dictSet("key2", 2);
      await target.listAdd("item1");
      await target.listAdd("item2");
      await target.queueEnqueue(100);
      await target.queueEnqueue(200);
      await target.setAdd("set1");
      await target.setAdd("set2");

      expect(await target.dictSize()).toBe(2);
      expect(await target.listLength()).toBe(2);
      expect(await target.queueSize()).toBe(2);
      expect(await target.setSize()).toBe(2);

      await deactivateOnIdleAndAssertFresh(target);

      expect(await target.dictSize()).toBe(2);
      expect(await target.dictGet("key1")).toBe(1);
      expect(await target.dictGet("key2")).toBe(2);

      expect(await target.listLength()).toBe(2);
      expect(await target.listGet(0)).toBe("item1");
      expect(await target.listGet(1)).toBe("item2");

      expect(await target.queueSize()).toBe(2);
      expect(await target.queuePeek()).toBe(100);

      expect(await target.setSize()).toBe(2);
      expect(await target.setToArray()).toEqual(expect.arrayContaining(["set1", "set2"]));
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_State_Modifications_Test",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await target.dictSet("key1", 1);
      await target.listAdd("item1");
      await target.queueEnqueue(100);
      await target.setAdd("set1");

      await target.dictSet("key2", 2);
      await target.dictSet("key1", 10); // Update via existing key.
      await target.listAdd("item2");
      await target.queueEnqueue(200);
      await target.setAdd("set2");

      expect(await target.dictSize()).toBe(2);
      expect(await target.dictGet("key1")).toBe(10);
      expect(await target.listLength()).toBe(2);
      expect(await target.queueSize()).toBe(2);
      expect(await target.setSize()).toBe(2);

      await deactivateOnIdleAndAssertFresh(target);

      expect(await target.dictSize()).toBe(2);
      expect(await target.dictGet("key1")).toBe(10);
      expect(await target.dictGet("key2")).toBe(2);

      expect(await target.listLength()).toBe(2);
      expect(await target.listGet(0)).toBe("item1");
      expect(await target.listGet(1)).toBe("item2");

      await target.dictDelete("key1");
      await target.listRemoveAt(0);
      await target.queueDequeue();
      await target.setDelete("set1");

      expect(await target.dictSize()).toBe(1);
      expect(await target.listLength()).toBe(1);
      expect(await target.queueSize()).toBe(1);
      expect(await target.setSize()).toBe(1);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.Grain_State_Should_Persist_Between_Activations",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await target.setValue({ name: "Test Name", counter: 42 });
      const initialState = await target.getValue();

      await deactivateOnIdleAndAssertFresh(target);

      const newState = await target.getValue();

      expect(newState).toEqual(initialState);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.Grain_Should_Handle_Multiple_Collections",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await target.dictSet("key1", 1);
      await target.dictSet("key2", 2);

      await target.listAdd("item1");
      await target.listAdd("item2");

      await target.queueEnqueue(100);
      await target.queueEnqueue(200);

      await target.setAdd("set1");
      await target.setAdd("set2");
      await target.setAdd("set1"); // Duplicate, should be ignored.

      expect(await target.dictSize()).toBe(2);
      expect(await target.listLength()).toBe(2);
      expect(await target.queueSize()).toBe(2);
      expect(await target.setSize()).toBe(2);

      await deactivateOnIdleAndAssertFresh(target);

      expect(await target.dictGet("key1")).toBe(1);
      expect(await target.dictGet("key2")).toBe(2);
      expect(await target.listGet(0)).toBe("item1");
      expect(await target.listGet(1)).toBe("item2");
      expect(await target.queuePeek()).toBe(100);
      expect(await target.setToArray()).toEqual(expect.arrayContaining(["set1", "set2"]));

      await target.dictDelete("key1");
      await target.listRemoveAt(0);
      await target.queueDequeue();
      await target.setDelete("set1");

      expect(await target.dictSize()).toBe(1);
      expect(await target.listLength()).toBe(1);
      expect(await target.queueSize()).toBe(1);
      expect(await target.setSize()).toBe(1);

      await deactivateOnIdleAndAssertFresh(target);

      expect(await target.dictSize()).toBe(1);
      expect(await target.listLength()).toBe(1);
      expect(await target.queueSize()).toBe(1);
      expect(await target.setSize()).toBe(1);
      expect(await target.dictGet("key2")).toBe(2);
      expect(await target.listGet(0)).toBe("item2");
      expect(await target.queuePeek()).toBe(200);
      expect(await target.setToArray()).toEqual(["set2"]);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.DurableGrainTests.Grain_Should_Handle_Large_State",
    async () => {
      const target = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      const itemCount = 1000;
      for (let i = 0; i < itemCount; i++) {
        await target.dictSet(`key${i}`, i);
        if (i < 100) {
          await target.listAdd(`item${i}`);
          await target.queueEnqueue(i);
          await target.setAdd(`set${i}`);
        }
      }

      expect(await target.dictSize()).toBe(itemCount);
      expect(await target.listLength()).toBe(100);
      expect(await target.queueSize()).toBe(100);
      expect(await target.setSize()).toBe(100);

      await deactivateOnIdleAndAssertFresh(target);

      for (let i = 0; i < 10; i++) {
        const randomIndex = Math.floor(Math.random() * (itemCount - 1));
        expect(await target.dictGet(`key${randomIndex}`)).toBe(randomIndex);

        if (randomIndex < 100) {
          expect(await target.listGet(randomIndex)).toBe(`item${randomIndex}`);
          expect(await target.setToArray()).toContain(`set${randomIndex}`);
        }
      }
    },
  );
});
