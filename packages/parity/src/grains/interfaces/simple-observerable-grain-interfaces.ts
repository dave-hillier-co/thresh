// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISimpleObserverableGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { ISimpleGrain } from "@thresh/parity/grains/interfaces/simple-grain-interfaces";

export { ISimpleGrain };

/**
 * A client-hosted callback target (Orleans `ISimpleGrainObserver`). `stateChanged`
 * is one-way: Orleans observer notifications are fire-and-forget, so the grain
 * does not await the client's acknowledgement.
 */
export interface ISimpleGrainObserver extends GrainKey<string> {
  stateChanged(a: number, b: number): Promise<void>;
}

export const ISimpleGrainObserver = defineGrainInterface<ISimpleGrainObserver>(
  "UnitTests.GrainInterfaces.ISimpleGrainObserver",
  { options: { stateChanged: { oneWay: true } } },
);

export interface ISimpleObserverableGrain extends ISimpleGrain {
  subscribe(observer: ISimpleGrainObserver): Promise<void>;
  unsubscribe(observer: ISimpleGrainObserver): Promise<void>;
  getRuntimeInstanceId(): Promise<string>;
}

export const ISimpleObserverableGrain = defineGrainInterface<ISimpleObserverableGrain>(
  "UnitTests.GrainInterfaces.ISimpleObserverableGrain",
);
