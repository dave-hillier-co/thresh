// Ported from dotnet/orleans test/Grains/TestGrains/GenericGrains.cs (LongRunningTaskGrain<T>) @ v10.1.0 (MIT).
// See long-running-task-grain-interfaces.ts for what was narrowed and why.
import { grain } from "@tsva/core/decorators";
import { GrainTaskCanceledError } from "@tsva/core/errors";
import { Grain } from "@tsva/core/grain";
import type { GrainCancellationToken } from "@tsva/core/grain-cancellation-token";
import { ILongRunningTaskGrain } from "@tsva/parity/grains/interfaces/long-running-task-grain-interfaces";

export { ILongRunningTaskGrain };

/**
 * Await `delayMs`, honouring `token` (Orleans `await Task.Delay(delay, ct.CancellationToken)`):
 * resolves normally if the delay elapses first, rejects with
 * `GrainTaskCanceledError` the moment the token's signal fires (including
 * one that already fired before this was called — mirrors
 * `cancellation.cluster.test.ts`'s cancellable-delay helper).
 */
function cancellableDelay(token: GrainCancellationToken, delayMs: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (token.isCancellationRequested) {
      reject(new GrainTaskCanceledError());
      return;
    }
    const timer = setTimeout(resolve, delayMs);
    token.signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new GrainTaskCanceledError());
      },
      { once: true },
    );
  });
}

@grain({ name: "UnitTests.Grains.LongRunningTaskGrain" })
export class LongRunningTaskGrain extends Grain implements ILongRunningTaskGrain {
  private readonly cancelledCallIds = new Set<string>();

  async longWaitGrainCancellation(
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void> {
    try {
      await cancellableDelay(token, delayMs);
    } catch (error) {
      this.cancelledCallIds.add(callId);
      throw error;
    }
  }

  async longWaitGrainCancellationInterleaving(
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void> {
    return this.longWaitGrainCancellation(token, delayMs, callId);
  }

  async callOtherLongRunningTaskGrainCancellation(
    target: ILongRunningTaskGrain,
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void> {
    await target.longWaitGrainCancellation(token, delayMs, callId);
  }

  async wasCancelled(callId: string): Promise<boolean> {
    return this.cancelledCallIds.has(callId);
  }

  // "Plain CancellationToken" surface, ported from `CancellationTokenTests.cs`.
  // JS has only one cancellation-token type (`GrainCancellationToken` wrapping
  // `AbortSignal`), so these bodies are identical to their
  // `...GrainCancellation` counterparts above.
  async longWait(token: GrainCancellationToken, delayMs: number, callId: string): Promise<void> {
    return this.longWaitGrainCancellation(token, delayMs, callId);
  }

  async longWaitInterleaving(
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void> {
    return this.longWait(token, delayMs, callId);
  }

  async callOtherLongRunningTask(
    target: ILongRunningTaskGrain,
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void> {
    await target.longWait(token, delayMs, callId);
  }
}
