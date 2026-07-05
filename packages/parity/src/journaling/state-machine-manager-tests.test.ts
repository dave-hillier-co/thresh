// Ported from dotnet/orleans test/Orleans.Journaling.Tests/StateMachineManagerTests.cs @ v10.1.0 (MIT).
//
// See durable-value-tests.test.ts for why this drives a real grain
// (DurableCollectionsGrain, which registers a value/dictionary/list/queue/set
// durable facet all on one manager) through TestCluster rather than the
// internal StateMachineManager surface directly (out of bounds for
// @tsva/parity), and why "new manager, same storage" becomes a silo restart.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  DurableCollectionsGrain,
  IDurableCollectionsGrain,
} from "@tsva/parity/grains/impl/durable-collections-grain";

describe("Orleans.Journaling.Tests.StateMachineManagerTests", () => {
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
  const freshKey = () => `smm-${nextKey++}`;

  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_RegisterStateMachine_Test",
    async () => {
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.dictSet("key1", 1);
      await grain.listAdd("item1");
      await grain.queueEnqueue(42);

      expect(await grain.dictGet("key1")).toBe(1);
      expect(await grain.listGet(0)).toBe("item1");
      expect(await grain.queuePeek()).toBe(42);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_StateRecovery_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.dictSet("key1", 1);
      await grain.dictSet("key2", 2);
      await grain.listAdd("item1");
      await grain.listAdd("item2");

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.dictSize()).toBe(2);
      expect(await grain2.dictGet("key1")).toBe(1);
      expect(await grain2.dictGet("key2")).toBe(2);

      expect(await grain2.listLength()).toBe(2);
      expect(await grain2.listGet(0)).toBe("item1");
      expect(await grain2.listGet(1)).toBe("item2");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_MultipleWriteStates_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      // Each mutator here already persists synchronously (no separate
      // WriteStateAsync-equivalent step), so the checkpoints upstream takes
      // between mutations are implicit.
      await grain.dictSet("key1", 1);
      await grain.dictSet("key2", 2);
      await grain.dictSet("key1", 10);
      await grain.dictDelete("key2");

      expect(await grain.dictSize()).toBe(1);
      expect(await grain.dictGet("key1")).toBe(10);
      expect(await grain.dictHas("key2")).toBe(false);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.dictSize()).toBe(1);
      expect(await grain2.dictGet("key1")).toBe(10);
      expect(await grain2.dictHas("key2")).toBe(false);
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_MultipleStateMachines_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      await grain.dictSet(1, "one");
      await grain.dictSet(2, "two");

      await grain.listAdd("item1");
      await grain.listAdd("item2");

      const person = { id: 100, name: "Test Person", age: 30 };
      await grain.setValue(person);

      expect(await grain.dictSize()).toBe(2);
      expect(await grain.dictGet(1)).toBe("one");

      expect(await grain.listLength()).toBe(2);
      expect(await grain.listGet(0)).toBe("item1");

      const value = (await grain.getValue()) as typeof person;
      expect(value).not.toBeNull();
      expect(value.id).toBe(100);
      expect(value.name).toBe("Test Person");

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.dictSize()).toBe(2);
      expect(await grain2.dictGet(1)).toBe("one");

      expect(await grain2.listLength()).toBe(2);
      expect(await grain2.listGet(0)).toBe("item1");

      const recoveredValue = (await grain2.getValue()) as typeof person;
      expect(recoveredValue).not.toBeNull();
      expect(recoveredValue.id).toBe(100);
      expect(recoveredValue.name).toBe("Test Person");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_Concurrency_Test",
    async () => {
      // Namespace isolation between two different state machines with similar
      // keys: here, the grain's own dictionary vs. its set (rather than two
      // separately-named dictionaries, since DurableCollectionsGrain exposes
      // one of each kind) — both keyed "key1"/"key2", independent stores.
      const grain = cluster.getGrain(IDurableCollectionsGrain, freshKey());

      await grain.dictSet("key1", 1);
      await grain.setAdd("key1");

      await grain.dictSet("key2", 2);
      await grain.setAdd("key2");

      expect(await grain.dictSize()).toBe(2);
      expect(await grain.setSize()).toBe(2);

      expect(await grain.dictGet("key1")).toBe(1);
      expect(await grain.setToArray()).toContain("key1");

      expect(await grain.dictGet("key2")).toBe(2);
      expect(await grain.setToArray()).toContain("key2");
    },
  );

  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_LargeStateRecovery_Test",
    async () => {
      const key = freshKey();
      const grain = cluster.getGrain(IDurableCollectionsGrain, key);

      const itemCount = 1000;
      for (let i = 0; i < itemCount; i++) await grain.dictSet(i, `Value ${i}`);

      await cluster.restartSilo(cluster.primary);
      const grain2 = cluster.getGrain(IDurableCollectionsGrain, key);

      expect(await grain2.dictSize()).toBe(itemCount);
      for (let i = 0; i < itemCount; i++) {
        expect(await grain2.dictGet(i)).toBe(`Value ${i}`);
      }
    },
    15_000,
  );

  // GAP: this exercises StateMachineManager's "retiring" state machines —
  // an unregistered structure's data is preserved for a configurable grace
  // period (and can be "un-retired" by re-registering it) before a
  // compaction purges it for good. StateMachineManagerImpl has no such
  // concept: `compact()` only ever writes snapshot frames for currently
  // registered machines, so an unregistered structure's entries are dropped
  // at the very next compaction with no grace period at all.
  orleansTest.gap(
    "GAP-STATE-MACHINE-RETIREMENT",
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_AutoRetiringStateMachines",
  );
});
