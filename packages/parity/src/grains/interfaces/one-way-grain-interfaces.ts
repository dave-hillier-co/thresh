// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ITestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";
import type { ISimpleGrainObserver } from "@thresh/parity/grains/interfaces/simple-observerable-grain-interfaces";

export interface IOneWayGrain extends GrainWithGuidKey {
  notify(observer: ISimpleGrainObserver): Promise<void>;
  // Upstream declares this as ValueTask; JS has no synchronous-Task
  // distinction, so it has the same async shape but is still one-way.
  notifyValueTask(observer: ISimpleGrainObserver): Promise<void>;
  throwsOneWay(): Promise<void>;
  throwsOneWayValueTask(): Promise<void>;
  notifyOtherGrain(otherGrain: IOneWayGrain, observer: ISimpleGrainObserver): Promise<boolean>;
  notifyOtherGrainValueTask(
    otherGrain: IOneWayGrain,
    observer: ISimpleGrainObserver,
  ): Promise<boolean>;
  getCount(): Promise<number>;
}

export const IOneWayGrain = defineGrainInterface<IOneWayGrain>(
  "UnitTests.GrainInterfaces.IOneWayGrain",
  {
    options: {
      notify: { oneWay: true },
      notifyValueTask: { oneWay: true },
      throwsOneWay: { oneWay: true },
      throwsOneWayValueTask: { oneWay: true },
    },
  },
);
