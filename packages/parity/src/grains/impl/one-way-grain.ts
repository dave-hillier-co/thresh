// Ported from dotnet/orleans test/Grains/TestInternalGrains/TestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { ISimpleGrainObserver } from "@tsva/parity/grains/interfaces/simple-observerable-grain-interfaces";
import type { IOneWayGrain } from "@tsva/parity/grains/interfaces/one-way-grain-interfaces";

@grain({ name: "UnitTests.Grains.OneWayGrain" })
export class OneWayGrain extends Grain implements IOneWayGrain {
  private count = 0;

  async notify(observer: ISimpleGrainObserver): Promise<void> {
    this.count++;
    await observer.stateChanged(this.count - 1, this.count);
  }

  async notifyValueTask(observer: ISimpleGrainObserver): Promise<void> {
    this.count++;
    await observer.stateChanged(this.count - 1, this.count);
  }

  async throwsOneWay(): Promise<void> {
    throw new Error("GET OUT!");
  }

  async throwsOneWayValueTask(): Promise<void> {
    throw new Error("GET OUT (ValueTask)!");
  }

  async notifyOtherGrain(otherGrain: IOneWayGrain, observer: ISimpleGrainObserver): Promise<boolean> {
    // Upstream returns whether the one-way call's Task had already reached
    // `RanToCompletion` by the time it was observed. JS has no synchronous
    // Task-completion state: a one-way proxy call always resolves to
    // `undefined` immediately after the message is dispatched, without
    // waiting on the callee. That fire-and-forget resolution is what `true`
    // stands in for here.
    const task = otherGrain.notify(observer);
    await task;
    return true;
  }

  async notifyOtherGrainValueTask(
    otherGrain: IOneWayGrain,
    observer: ISimpleGrainObserver,
  ): Promise<boolean> {
    const task = otherGrain.notifyValueTask(observer);
    await task;
    return true;
  }

  async getCount(): Promise<number> {
    return this.count;
  }
}
