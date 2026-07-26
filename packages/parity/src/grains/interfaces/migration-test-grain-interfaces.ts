// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/Migration/MigrationTests.cs @ v10.1.0 (MIT).
//
// Upstream declares these grain interfaces inline in the test file itself
// (namespace `DefaultCluster.Tests.General`), not under `UnitTests.GrainInterfaces`.
//
// Upstream's `GetGrainAddress()` (reporting the grain's own `GrainContext.Address`)
// has no analogue — nothing reaches a grain's own hosting silo from inside it
// (GAP-PLACEMENT-INTROSPECTION, see grain-placement-tests.test.ts) — so it is
// omitted; ported tests instead observe the hosting silo externally via
// `SiloHost.isActive`, the same pattern used there.
//
// Upstream triggers migration from the client via
// `grain.Cast<IGrainManagementExtension>().MigrateOnIdle()`, directed by a
// `RequestContext`-set placement hint. This framework's `GrainRuntime.migrateOnIdle`
// takes the target directly, so `migrateOnIdle(target?)` is exposed on the
// grain interface itself and forwards to it.
//
// Upstream's fail-dehydrate/fail-rehydrate signal travels via an ambient
// `RequestContext` flag the grain's `OnDehydrate` reads and rethrows from.
// This framework's grains hold that intent as their own instance state instead
// (set by an explicit `failNextRehydrate()` call before triggering migration),
// which is carried into the dehydration bag exactly the way upstream's grain
// itself does it internally.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";

export interface IMigrationTestGrain extends GrainWithIntegerKey {
  setState(state: number): Promise<void>;
  getState(): Promise<number>;
  migrateOnIdle(target?: SiloAddress): Promise<void>;
  /** Marks the *next* migration's rehydration to fail on the target silo. */
  failNextRehydrate(): Promise<void>;
  /**
   * Requests deactivation now, and asks the grain to call `migrateOnIdle(target)`
   * from its own `onDeactivate` hook when the sweep collects it.
   */
  migrateDuringDeactivation(target: SiloAddress): Promise<void>;
}

export const IMigrationTestGrain = defineGrainInterface<IMigrationTestGrain>(
  "DefaultCluster.Tests.General.IMigrationTestGrain",
);

/** Upstream's `Grain<TGrainState>`-backed variant: a single persistent-state facet. */
export interface IMigrationTestGrainGrainOfT extends GrainWithIntegerKey {
  setState(state: number): Promise<void>;
  getState(): Promise<number>;
  migrateOnIdle(target?: SiloAddress): Promise<void>;
}

export const IMigrationTestGrainGrainOfT = defineGrainInterface<IMigrationTestGrainGrainOfT>(
  "DefaultCluster.Tests.General.IMigrationTestGrain_GrainOfT",
);

export interface IMigrationTestGrainIPersistentStateOfT extends GrainWithIntegerKey {
  setState(a: number, b: number): Promise<void>;
  getState(): Promise<[number, number]>;
  migrateOnIdle(target?: SiloAddress): Promise<void>;
}

export const IMigrationTestGrainIPersistentStateOfT =
  defineGrainInterface<IMigrationTestGrainIPersistentStateOfT>(
    "DefaultCluster.Tests.General.IMigrationTestGrain_IPersistentStateOfT",
  );
