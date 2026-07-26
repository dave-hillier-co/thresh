// Ported from dotnet/orleans test/Grains/TestGrains/SimpleObserverableGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { grainReferenceIdentity } from "@thresh/core/grain-reference";
import { ObserverManager } from "@thresh/core/observer-manager";
import { systemTimeProvider } from "@thresh/core/time-provider";
import { SimpleGrain } from "@thresh/parity/grains/impl/simple-grain";
import type {
  ISimpleGrainObserver,
  ISimpleObserverableGrain,
} from "@thresh/parity/grains/interfaces/simple-observerable-grain-interfaces";

// Upstream raises the notification after a 1000ms `EventDelay`; this port
// notifies synchronously instead of porting a literal `Task.Delay` (there is
// nothing that delay defends against here — it only existed to make the
// upstream test's "no notification arrived yet" window observable).
@grain({ name: "UnitTests.Grains.SimpleObserverableGrain" })
export class SimpleObserverableGrain extends SimpleGrain implements ISimpleObserverableGrain {
  private readonly observers = new ObserverManager<string, ISimpleGrainObserver>(
    { minutes: 5 },
    systemTimeProvider,
  );

  async subscribe(observer: ISimpleGrainObserver): Promise<void> {
    const key = this.keyOf(observer);
    this.observers.subscribe(key, observer);
  }

  async unsubscribe(observer: ISimpleGrainObserver): Promise<void> {
    const key = this.keyOf(observer);
    this.observers.unsubscribe(key);
  }

  override async setA(a: number): Promise<void> {
    this.a = a;
    await this.raiseStateUpdate();
  }

  override async setB(b: number): Promise<void> {
    this.b = b;
    await this.raiseStateUpdate();
  }

  async getRuntimeInstanceId(): Promise<string> {
    return this.runtime.localSiloAddress().toString();
  }

  private async raiseStateUpdate(): Promise<void> {
    await this.observers.notify(async (o) => {
      await o.stateChanged(this.a, this.b);
    });
  }

  private keyOf(observer: ISimpleGrainObserver): string {
    const identity = grainReferenceIdentity(observer);
    if (identity === undefined) {
      throw new Error("subscribe/unsubscribe requires a grain reference observer");
    }
    return identity.grainId.toString();
  }
}
