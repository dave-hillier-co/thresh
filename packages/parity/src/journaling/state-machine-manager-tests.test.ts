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
import {
  RetiringCollectionsGrain,
  RetiringCollectionsGrainPartial,
} from "@tsva/parity/grains/impl/retiring-collections-grain";
import {
  IRetiringCollectionsGrain,
  IRetiringCollectionsGrainPartial,
} from "@tsva/parity/grains/interfaces/retiring-collections-grain-interfaces";

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

  // Exercises StateMachineManager's "retiring" state machines — an
  // unregistered structure's data is preserved for a grace period (and can be
  // "un-retired" by re-registering it) before a compaction purges it for good.
  //
  // Upstream constructs a fresh `IStateMachineManager` over the same storage
  // at each step, registering a different set of machines each time (a
  // "dictToKeep" that is always registered, and a "dictToRetire" that comes
  // and goes) to simulate a durable structure being removed from a grain's
  // dependencies across a deploy, then drives a real grace period via a
  // `FakeTimeProvider`.
  //
  // Reaching the manager surface directly is out of bounds here (see the file
  // banner above), and the grain activation path has no way to plumb a fake
  // clock down to `StateMachineManagerImpl` without editing the hosting layer
  // (out of scope for this gap). So this drives the same scenario through two
  // grain *classes* that share one grain type name
  // ("Orleans.Journaling.Tests.RetiringCollectionsGrain": see
  // retiring-collections-grain.ts) — one with both durable dictionaries, one
  // with only "keep" — resolving to the *same* `GrainId` over a shared
  // journal-storage instance passed between separately-started `TestCluster`s
  // (the same "new manager, same storage" pattern the rest of this file uses
  // `restartSilo` for). The grace period itself is expressed as a small,
  // fixed number of compactions an unregistered structure survives (default
  // 2: `StateMachineManagerImpl`'s `retirementGraceCompactions`) rather than
  // upstream's `TimeSpan`, and is driven by crossing the manager's ordinary
  // snapshot threshold repeatedly instead of advancing a clock.
  orleansTest(
    "Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_AutoRetiringStateMachines",
    async () => {
      const key = freshKey();
      const clusters: TestCluster[] = [];
      type ConfigureSilo = NonNullable<Parameters<typeof TestCluster.start>[0]>["configureSilo"];
      const startFull = (configureSilo?: ConfigureSilo) =>
        TestCluster.start({
          initialSilos: 1,
          grains: [{ ctor: RetiringCollectionsGrain, interfaces: [IRetiringCollectionsGrain] }],
          ...(configureSilo !== undefined ? { configureSilo } : {}),
        });
      const startPartial = (configureSilo?: ConfigureSilo) =>
        TestCluster.start({
          initialSilos: 1,
          grains: [
            {
              ctor: RetiringCollectionsGrainPartial,
              interfaces: [IRetiringCollectionsGrainPartial],
            },
          ],
          ...(configureSilo !== undefined ? { configureSilo } : {}),
        });
      // Force a value past the manager's default snapshot threshold (100),
      // triggering exactly one compaction cycle.
      const pumpKeep = async (
        grain: { keepSet(key: unknown, value: unknown): Promise<void> },
        count: number,
        start: number,
      ) => {
        for (let i = 0; i < count; i += 1) await grain.keepSet("a", start + i);
      };

      try {
        // -------------- STEP 1 --------------
        // Both "keep" and "retire" registered: as-shipped, before "retire" is
        // ever removed from the grain's dependencies.
        const cluster1 = await startFull();
        clusters.push(cluster1);
        const g1 = cluster1.getGrain(IRetiringCollectionsGrain, key);
        await g1.keepSet("a", 0);
        await g1.retireSet("b", 1);

        // Every later cluster shares this storage, standing in for "the same
        // durable backend across silo restarts / deploys" the rest of this
        // file gets from `restartSilo`.
        const sharedJournal = cluster1.journalStorage;

        // -------------- STEP 2 --------------
        // Only "keep" is registered from here on: "retire" is now an orphan
        // (removed from the grain's dependencies, per the file banner above).
        const cluster2 = await startPartial((builder) =>
          builder.useMemoryJournaling(sharedJournal),
        );
        clusters.push(cluster2);
        const g2 = cluster2.getGrain(IRetiringCollectionsGrainPartial, key);
        expect(await g2.keepGet("a")).toBe(0);

        // Cross the snapshot threshold once: 1 compaction while "retire" is
        // unregistered. Within the grace period (2), so it must be kept.
        await pumpKeep(g2, 100, 1);

        // -------------- STEP 3 --------------
        // Verify the retired dictionary was NOT purged, then re-register it
        // (resurrecting it) and cross the threshold again so the resurrection
        // is durably persisted as an ordinary snapshot.
        const cluster3 = await startFull((builder) => builder.useMemoryJournaling(sharedJournal));
        clusters.push(cluster3);
        const g3 = cluster3.getGrain(IRetiringCollectionsGrain, key);

        expect(await g3.keepGet("a")).toBe(100); // last value pumped in step 2
        // The fact this entry exists proves "retire"'s state survived step 2's
        // compaction even though it was not registered there.
        expect(await g3.retireGet("b")).toBe(1);

        await g3.retireSet("b", 2);
        await pumpKeep(g3, 100, 200); // crosses the threshold: persists the resurrection for good

        // -------------- STEP 4 --------------
        // Because re-registering "retire" in step 3 resurrected it, this is
        // like step 2: only "keep" is registered, so "retire" is unregistered
        // again — but its grace period starts fresh (it was cleared on
        // resurrection). Cross the threshold twice while it stays
        // unregistered: the 2nd compaction (grace period elapsed) purges it.
        const cluster4 = await startPartial((builder) =>
          builder.useMemoryJournaling(sharedJournal),
        );
        clusters.push(cluster4);
        const g4 = cluster4.getGrain(IRetiringCollectionsGrainPartial, key);
        await pumpKeep(g4, 250, 300);

        // -------------- STEP 5 --------------
        // Re-register both. "keep" has the latest data; "retire" comes back
        // empty because its data was purged for good during step 4.
        const cluster5 = await startFull((builder) => builder.useMemoryJournaling(sharedJournal));
        clusters.push(cluster5);
        const g5 = cluster5.getGrain(IRetiringCollectionsGrain, key);

        expect(await g5.keepGet("a")).toBe(549); // last value pumped in step 4
        expect(await g5.retireHas("b")).toBe(false);
      } finally {
        await Promise.all(clusters.map((c) => c.dispose()));
      }
    },
    30_000,
  );
});
