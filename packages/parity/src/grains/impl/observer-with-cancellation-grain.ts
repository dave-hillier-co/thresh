// Ported from dotnet/orleans test/Grains/TestGrains/ObserverWithCancellationGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { GrainCancellationToken } from "@thresh/core/grain-cancellation-token";
import {
  ILongRunningObserver,
  IObserverWithCancellationGrain,
} from "@thresh/parity/grains/interfaces/observer-with-cancellation-grain-interfaces";

export { ILongRunningObserver, IObserverWithCancellationGrain };

@grain({ name: "UnitTests.Grains.ObserverWithCancellationGrain" })
export class ObserverWithCancellationGrain extends Grain implements IObserverWithCancellationGrain {
  private observer: ILongRunningObserver | undefined;
  private readonly processedCancellations: string[] = [];

  async subscribe(observer: ILongRunningObserver): Promise<void> {
    this.observer = observer;
  }

  async unsubscribe(observer: ILongRunningObserver): Promise<void> {
    if (this.observer === observer) this.observer = undefined;
  }

  async notifyLongWait(
    delayMs: number,
    callId: string,
    token: GrainCancellationToken,
  ): Promise<void> {
    if (this.observer === undefined) throw new Error("No observer subscribed.");
    try {
      await this.observer.longWait(delayMs, callId, token);
    } catch (err) {
      this.processedCancellations.push(callId);
      throw err;
    }
  }

  async notifyInterleavingLongWait(
    delayMs: number,
    callId: string,
    token: GrainCancellationToken,
  ): Promise<void> {
    if (this.observer === undefined) throw new Error("No observer subscribed.");
    try {
      await this.observer.interleavingLongWait(delayMs, callId, token);
    } catch (err) {
      this.processedCancellations.push(callId);
      throw err;
    }
  }

  async getProcessedCancellations(): Promise<string[]> {
    return [...this.processedCancellations];
  }
}
