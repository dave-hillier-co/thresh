// Ported from dotnet/orleans test/Orleans.Runtime.Tests/DeactivationTracingTests.cs
// @ v10.1.0 (MIT). Upstream declares each grain interface inline in the test
// file itself, along with `ActivityData` (not needed here — the OnDeactivate
// span assertions read the harness's captured spans, not data the grain
// reports back; only `getActivityId`'s SPAN ID matters, for parity with the
// activation-tracing grains' shape).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";
import type { SiloAddress } from "@tsva/core/silo-address";

/** Basic deactivation tracing: a plain `onDeactivate` no-op. */
export interface IDeactivationTracingTestGrain extends GrainWithIntegerKey {
  getActivityId(): Promise<string | undefined>;
}

export const IDeactivationTracingTestGrain = defineGrainInterface<IDeactivationTracingTestGrain>(
  "UnitTests.GrainInterfaces.IDeactivationTracingTestGrain",
);

/** Deactivation tracing with a persistent-state write from inside `onDeactivate`. */
export interface IDeactivationWithWorkTracingTestGrain extends GrainWithIntegerKey {
  getActivityId(): Promise<string | undefined>;
  wasDeactivated(): Promise<boolean>;
}

export const IDeactivationWithWorkTracingTestGrain =
  defineGrainInterface<IDeactivationWithWorkTracingTestGrain>(
    "UnitTests.GrainInterfaces.IDeactivationWithWorkTracingTestGrain",
  );

/** Deactivation tracing where `onDeactivate` throws. */
export interface IDeactivationWithExceptionTracingTestGrain extends GrainWithIntegerKey {
  getActivityId(): Promise<string | undefined>;
}

export const IDeactivationWithExceptionTracingTestGrain =
  defineGrainInterface<IDeactivationWithExceptionTracingTestGrain>(
    "UnitTests.GrainInterfaces.IDeactivationWithExceptionTracingTestGrain",
  );

/** A grain whose constructor throws, so activation never completes. */
export interface IActivationFailureDeactivationGrain extends GrainWithIntegerKey {
  getActivityId(): Promise<string | undefined>;
}

export const IActivationFailureDeactivationGrain =
  defineGrainInterface<IActivationFailureDeactivationGrain>(
    "UnitTests.GrainInterfaces.IActivationFailureDeactivationGrain",
  );

/**
 * Deactivation tracing with a migration participant, so a migration sweep's
 * `OnDeactivate` span (with the "migrating" reason) can be asserted alongside
 * the dehydrate span it precedes. `migrateOnIdle(target?)` mirrors
 * `IMigrationTestGrain`'s shape (this framework's `GrainRuntime.migrateOnIdle`
 * takes the target directly, unlike upstream's placement-hint-directed
 * `Cast<IGrainManagementExtension>().MigrateOnIdle()`).
 */
export interface IDeactivationMigrationTracingTestGrain extends GrainWithIntegerKey {
  setState(state: number): Promise<void>;
  getState(): Promise<number>;
  migrateOnIdle(target?: SiloAddress): Promise<void>;
}

export const IDeactivationMigrationTracingTestGrain =
  defineGrainInterface<IDeactivationMigrationTracingTestGrain>(
    "UnitTests.GrainInterfaces.IDeactivationMigrationTracingTestGrain",
  );
