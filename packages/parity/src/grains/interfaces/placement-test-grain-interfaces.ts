// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IPlacementTestGrain.cs @ v10.1.0 (MIT).
//
// Upstream `IPlacementTestGrain` also declares GetEndpoint/GetActivationId,
// only ported here as needed by the ported tests that exercise them: the
// placement-forcing members, `GetRuntimeInstanceId` (a grain reporting its
// own hosting silo, via `GrainRuntime.localSiloAddress()`), and the
// load-shedding EnableOverloadDetection/Latch/Unlatch test hooks
// (`GrainRuntime.enableOverloadDetection`/`latchCpuUsage`/etc.) every
// `PlacementTestGrainBase`-derived grain implements upstream — used by
// `ElasticPlacementTests.ElasticityGrainPlacementTest`, which taints/restores
// *the pinned grain's own hosting silo* (an `IRandomPlacementTestGrain`
// pinned there via `getGrainAtSilo`), then separately samples placement
// through `IActivationCountBasedPlacementTestGrain`.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { Guid } from "@thresh/core/guid";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";

export interface IPlacementTestGrain extends GrainWithGuidKey {
  nop(): Promise<void>;
  startPreferLocalGrain(key: Guid): Promise<Guid>;
  /** Upstream `GetRuntimeInstanceId()`: this activation's hosting silo identity. */
  getRuntimeInstanceId(): Promise<string>;
  /** Upstream `GetActivationId()`: a per-activation identity, stable for this activation's lifetime. */
  getActivationId(): Promise<string>;
  /**
   * Upstream `StartLocalGrains`: force-activates an `IStatelessWorkerPlacementTestGrain`
   * per key by calling `Nop()` on each, so a caller can then sample where they landed.
   */
  startLocalGrains(keys: Guid[]): Promise<void>;
  /**
   * Upstream `SampleLocalGrainEndpoint`: calls `GetRuntimeInstanceId()` (this port's
   * `IPEndPoint` stand-in — see `grain-placement-tests.test.ts`'s header) on the
   * `IStatelessWorkerPlacementTestGrain` keyed by `key`, `sampleSize` times, to sample
   * which activation(s) answer stateless-worker calls for that key.
   */
  sampleLocalGrainEndpoint(key: Guid, sampleSize: number): Promise<string[]>;
  enableOverloadDetection(enabled: boolean): Promise<void>;
  latchOverloaded(): Promise<void>;
  unlatchOverloaded(): Promise<void>;
  latchCpuUsage(value: number): Promise<void>;
  unlatchCpuUsage(): Promise<void>;
}

export const IPlacementTestGrain = defineGrainInterface<IPlacementTestGrain>(
  "UnitTests.GrainInterfaces.IPlacementTestGrain",
);

export type IRandomPlacementTestGrain = IPlacementTestGrain;

export const IRandomPlacementTestGrain = defineGrainInterface<IRandomPlacementTestGrain>(
  "UnitTests.GrainInterfaces.IRandomPlacementTestGrain",
);

export type IPreferLocalPlacementTestGrain = IPlacementTestGrain;

export const IPreferLocalPlacementTestGrain = defineGrainInterface<IPreferLocalPlacementTestGrain>(
  "UnitTests.GrainInterfaces.IPreferLocalPlacementTestGrain",
);

/**
 * Upstream `IActivationCountBasedPlacementTestGrain`: an `IPlacementTestGrain`
 * placed by `ActivationCountPlacement`, this port's load-aware strategy —
 * the type `ElasticPlacementTests.ElasticityGrainPlacementTest` samples
 * placement through once the tainted silo is set up.
 */
export type IActivationCountBasedPlacementTestGrain = IPlacementTestGrain;

export const IActivationCountBasedPlacementTestGrain =
  defineGrainInterface<IActivationCountBasedPlacementTestGrain>(
    "UnitTests.GrainInterfaces.IActivationCountBasedPlacementTestGrain",
  );

/**
 * Upstream `IStatelessWorkerPlacementTestGrain`: an `IPlacementTestGrain`
 * placed by `[StatelessWorker(1)]` (`stateless: true, maxLocalWorkers: 1`),
 * used by `GrainPlacementTests.StatelessWorkerShouldCreateSpecifiedActivationCount`
 * and `.StatelessWorkerGrainShouldCreateActivationsOnLocalSilo`.
 */
export type IStatelessWorkerPlacementTestGrain = IPlacementTestGrain;

export const IStatelessWorkerPlacementTestGrain =
  defineGrainInterface<IStatelessWorkerPlacementTestGrain>(
    "UnitTests.GrainInterfaces.IStatelessWorkerPlacementTestGrain",
  );

/** Upstream `IOtherStatelessWorkerPlacementTestGrain`: `[StatelessWorker(2)]`. */
export type IOtherStatelessWorkerPlacementTestGrain = IPlacementTestGrain;

export const IOtherStatelessWorkerPlacementTestGrain =
  defineGrainInterface<IOtherStatelessWorkerPlacementTestGrain>(
    "UnitTests.GrainInterfaces.IOtherStatelessWorkerPlacementTestGrain",
  );
