// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/Migration/MigrationTests.cs @ v10.1.0 (MIT).
//
// Upstream loops each case up to 100 times (partly stress-testing the
// migration path, partly retrying until an undirected migration happens to
// land somewhere new). This framework's undirected migration always excludes
// the local silo when choosing among the cluster (see `chooseMigrationTarget`),
// so a single attempt is guaranteed to move the activation; loop counts are
// reduced accordingly without weakening any assertion inside the loop.
//
// Migration only runs on the collection sweep once an activation goes stale
// (see `ActivationData.isStale`), so every case sets a short per-grain
// `collectionAgeSeconds` and drives a `FakeTimeProvider` past it plus the
// default collection interval to force a sweep.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { GrainId } from "@tsva/core/grain-id";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import {
  IMigrationTestGrain,
  IMigrationTestGrainGrainOfT,
  IMigrationTestGrainIPersistentStateOfT,
  MigrationTestGrain,
  MigrationTestGrainWithInjectedMemoryStorage,
  MigrationTestGrainWithMemoryStorage,
} from "@tsva/parity/grains/impl/migration-test-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

const migrationGrainType = getGrainMetadata(MigrationTestGrain)!.grainType;
const grainOfTGrainType = getGrainMetadata(MigrationTestGrainWithMemoryStorage)!.grainType;
const persistentStateOfTGrainType = getGrainMetadata(
  MigrationTestGrainWithInjectedMemoryStorage,
)!.grainType;

function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
  return cluster.silos.find((s) => s.host.isActive(grainId));
}

// Fires the collection sweep (default interval 60s) well past the 1s
// `collectionAgeSeconds` every migration-test grain is registered with.
async function forceMigrationSweep(time: FakeTimeProvider): Promise<void> {
  time.advance(65_000);
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("DefaultCluster.Tests.General.MigrationTests", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [
        { ctor: MigrationTestGrain, interfaces: [IMigrationTestGrain] },
        { ctor: MigrationTestGrainWithMemoryStorage, interfaces: [IMigrationTestGrainGrainOfT] },
        {
          ctor: MigrationTestGrainWithInjectedMemoryStorage,
          interfaces: [IMigrationTestGrainIPersistentStateOfT],
        },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.General.MigrationTests.BasicGrainMigrationTest", async () => {
    for (let i = 0; i < 3; i++) {
      const key = randomIntegerKey();
      const grain = cluster.getGrain(IMigrationTestGrain, key);
      const grainId = new GrainId(migrationGrainType, key);
      const expectedState = Math.floor(Math.random() * 1_000_000);

      // Force activation so there is an original host to move away from.
      await grain.setState(0);
      const originalHost = hostOf(cluster, grainId);

      await grain.migrateOnIdle();
      await grain.setState(expectedState);
      await forceMigrationSweep(time);
      await waitFor(() => {
        const host = hostOf(cluster, grainId);
        return host !== undefined && host !== originalHost;
      });

      expect(await grain.getState()).toBe(expectedState);
    }
  });

  orleansTest(
    "DefaultCluster.Tests.General.MigrationTests.InitiateMigrationFromOnDeactivateAsyncTest",
    async () => {
      for (let i = 0; i < 3; i++) {
        const key = randomIntegerKey();
        const grain = cluster.getGrain(IMigrationTestGrain, key);
        const grainId = new GrainId(migrationGrainType, key);
        const expectedState = Math.floor(Math.random() * 1_000_000);

        await grain.setState(expectedState);
        const originalHost = hostOf(cluster, grainId);
        const targetHost = cluster.silos.find((s) => s !== originalHost)!;

        // Trigger deactivation and tell the grain that it should migrate to the
        // target host from its own onDeactivate hook.
        await grain.migrateDuringDeactivation(targetHost.address);
        await forceMigrationSweep(time);
        await waitFor(() => hostOf(cluster, grainId) === targetHost);

        expect(hostOf(cluster, grainId)).toBe(targetHost);
        expect(await grain.getState()).toBe(expectedState);
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.MigrationTests.DirectedGrainMigrationTest",
    async () => {
      for (let i = 0; i < 3; i++) {
        const key = randomIntegerKey();
        const grain = cluster.getGrain(IMigrationTestGrain, key);
        const grainId = new GrainId(migrationGrainType, key);
        const expectedState = Math.floor(Math.random() * 1_000_000);

        await grain.setState(expectedState);
        const originalHost = hostOf(cluster, grainId);
        const targetHost = cluster.silos.find((s) => s !== originalHost)!;

        await grain.migrateOnIdle(targetHost.address);
        await forceMigrationSweep(time);
        await waitFor(() => hostOf(cluster, grainId) === targetHost);

        expect(hostOf(cluster, grainId)).toBe(targetHost);
        expect(await grain.getState()).toBe(expectedState);
      }
    },
  );

  // Uses `RequestContext.Set(IPlacementDirector.PlacementHintKey, targetHost)`
  // from the *client* (test method) to steer placement, then migrates via the
  // cast-to-extension `MigrateOnIdle()` with no explicit target — two distinct
  // gaps stacked: client-side ambient RequestContext (GAP-CLIENT-REQUEST-CONTEXT,
  // an escape hatch this pass intentionally left alone — see
  // request-context-test.test.ts) and RequestContext-driven placement hints,
  // which this framework has no equivalent for (`migrateOnIdle` instead takes
  // an explicit `targetSilo`, already exercised by `DirectedGrainMigrationTest`
  // above). Un-gapping this needs both, not just the grain-facing
  // `requestContext` exposure this pass added.
  orleansTest.gap(
    "GAP-REQUEST-CONTEXT",
    "DefaultCluster.Tests.General.MigrationTests.MultiGrainDirectedMigrationTest",
  );

  orleansTest(
    "DefaultCluster.Tests.General.MigrationTests.DirectedGrainMigrationTest_GrainOfT",
    async () => {
      for (let i = 0; i < 3; i++) {
        const key = randomIntegerKey();
        const grain = cluster.getGrain(IMigrationTestGrainGrainOfT, key);
        const grainId = new GrainId(grainOfTGrainType, key);
        const expectedState = Math.floor(Math.random() * 1_000_000);

        await grain.setState(expectedState);
        const originalHost = hostOf(cluster, grainId);
        const targetHost = cluster.silos.find((s) => s !== originalHost)!;

        await grain.migrateOnIdle(targetHost.address);
        await forceMigrationSweep(time);
        await waitFor(() => hostOf(cluster, grainId) === targetHost);

        expect(hostOf(cluster, grainId)).toBe(targetHost);
        expect(await grain.getState()).toBe(expectedState);
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.MigrationTests.DirectedGrainMigrationTest_IPersistentStateOfT",
    async () => {
      for (let i = 0; i < 3; i++) {
        const key = randomIntegerKey();
        const grain = cluster.getGrain(IMigrationTestGrainIPersistentStateOfT, key);
        const grainId = new GrainId(persistentStateOfTGrainType, key);
        const expectedA = Math.floor(Math.random() * 1_000_000);
        const expectedB = Math.floor(Math.random() * 1_000_000);

        await grain.setState(expectedA, expectedB);
        const originalHost = hostOf(cluster, grainId);
        const targetHost = cluster.silos.find((s) => s !== originalHost)!;

        await grain.migrateOnIdle(targetHost.address);
        await forceMigrationSweep(time);
        await waitFor(() => hostOf(cluster, grainId) === targetHost);

        expect(hostOf(cluster, grainId)).toBe(targetHost);
        const [actualA, actualB] = await grain.getState();
        expect(actualA).toBe(expectedA);
        expect(actualB).toBe(expectedB);
      }
    },
  );

  // Same client-side `RequestContext`-as-placement-hint pattern as
  // `MultiGrainDirectedMigrationTest` above (`RequestContext.Set(...targetHost)`
  // from the test method), plus `RequestContext.Set("fail_dehydrate", true)`
  // from the client to tell the grain to fail its next dehydration — both
  // client-side ambient-context uses this framework has no equivalent for yet.
  orleansTest.gap(
    "GAP-REQUEST-CONTEXT",
    "DefaultCluster.Tests.General.MigrationTests.FailDehydrationTest",
  );

  // After a failed `onRehydrate` the target's directory entry still points at
  // the (now-invalid) migrated activation. The next call routes there via the
  // directory, finds it invalid, removes the stale pointer, and reactivates
  // fresh on that same silo (without the carried state) — rather than recursing
  // without bound between the directory and this silo. See the deliverLocal
  // stale-pointer repair in packages/runtime/src/distributed-dispatcher.ts.
  orleansTest("DefaultCluster.Tests.General.MigrationTests.FailRehydrationTest", async () => {
    const key = randomIntegerKey();
    const grain = cluster.getGrain(IMigrationTestGrain, key);
    const grainId = new GrainId(migrationGrainType, key);
    const expectedState = Math.floor(Math.random() * 1_000_000);

    await grain.setState(expectedState);
    const originalHost = hostOf(cluster, grainId);
    const targetHost = cluster.silos.find((s) => s !== originalHost)!;

    await grain.failNextRehydrate();
    await grain.migrateOnIdle(targetHost.address);
    await forceMigrationSweep(time);

    // Rehydration fails on the target, so the migrated activation is
    // discarded there; the directory still points at the target, so the next
    // call reactivates fresh (without the carried state) on that same silo.
    await waitFor(async () => {
      try {
        await grain.getState();
      } catch {
        return false;
      }
      return hostOf(cluster, grainId) === targetHost;
    });

    expect(hostOf(cluster, grainId)).toBe(targetHost);
    // The grain lost its state during the failed migration.
    expect(await grain.getState()).not.toBe(expectedState);
  });
});
