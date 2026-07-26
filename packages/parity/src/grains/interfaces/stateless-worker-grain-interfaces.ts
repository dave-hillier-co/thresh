// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IStatelessWorkerGrain.cs
// and test/Grains/TestGrainInterfaces/IStatelessWorkerScalingGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

/** Upstream `IStatelessWorkerGrain`: `[StatelessWorker(1)]`, exercised for capacity/queuing behaviour. */
export interface IStatelessWorkerGrain extends GrainWithIntegerKey {
  longCall(): Promise<void>;
  /** `[activationGuid, silo, calls]`, upstream's `Tuple<Guid, string, List<Tuple<DateTime,DateTime>>>`. */
  getCallStats(): Promise<[string, string, Array<[number, number]>]>;
  dummyCall(): Promise<void>;
}

export const IStatelessWorkerGrain = defineGrainInterface<IStatelessWorkerGrain>(
  "UnitTests.GrainInterfaces.IStatelessWorkerGrain",
);

/** Upstream `IStatelessWorkerScalingGrain`: `[StatelessWorker(maxLocalWorkers: 4)]`, exercised for scale-up behaviour. */
export interface IStatelessWorkerScalingGrain extends GrainWithIntegerKey {
  wait(): Promise<void>;
  release(): Promise<void>;
  getActivationCount(): Promise<number>;
  getWaitingCount(): Promise<number>;
}

export const IStatelessWorkerScalingGrain = defineGrainInterface<IStatelessWorkerScalingGrain>(
  "UnitTests.GrainInterfaces.IStatelessWorkerScalingGrain",
);
