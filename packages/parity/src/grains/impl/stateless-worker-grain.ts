// Ported from dotnet/orleans test/Grains/TestGrains/StatelessWorkerGrain.cs
// and test/Grains/TestGrains/StatelessWorkerScalingGrain.cs @ v10.1.0 (MIT).
import { grain, mayInterleave } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { Guid } from "@thresh/core/guid";
import type { ActivationReason } from "@thresh/core/reasons";
import {
  IStatelessWorkerGrain,
  IStatelessWorkerScalingGrain,
} from "@thresh/parity/grains/interfaces/stateless-worker-grain-interfaces";

export { IStatelessWorkerGrain, IStatelessWorkerScalingGrain };

/**
 * A counting semaphore (upstream: `SemaphoreSlim(0)`), shared by every
 * activation of `StatelessWorkerScalingGrain` regardless of grain key —
 * upstream injects ONE `StatelessWorkerScalingGrainSharedState` singleton per
 * silo, so `Wait()`/`Release()` pair up across activations, not per-id.
 */
class CountingSemaphore {
  private count = 0;
  private readonly waiters: Array<() => void> = [];

  wait(): Promise<void> {
    if (this.count > 0) {
      this.count -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  release(): void {
    const next = this.waiters.shift();
    if (next !== undefined) next();
    else this.count += 1;
  }
}

const scalingSemaphore = new CountingSemaphore();
/** Per-grain-id activation counts, upstream's `ConcurrentDictionary<GrainId, int>`. */
const scalingActivationCounts = new Map<string, number>();
/** Per-grain-id count of calls currently parked in `Wait()`. */
const scalingWaitingActivations = new Map<string, number>();

/**
 * Ported from `StatelessWorkerScalingGrain`: each activation blocks in
 * `Wait()` until released, so a test can hold N activations busy at once and
 * observe the catalog scale up to meet the load. `Release`/`GetActivationCount`/
 * `GetWaitingCount` are `[AlwaysInterleave]` upstream so they can still be
 * observed/answered while another call on the same activation is blocked in
 * `Wait()`.
 */
@grain({
  name: "UnitTests.Grains.StatelessWorkerScalingGrain",
  stateless: true,
  maxLocalWorkers: 4,
})
@mayInterleave((methodName) => methodName !== "wait")
export class StatelessWorkerScalingGrain extends Grain implements IStatelessWorkerScalingGrain {
  override async onActivate(_reason: ActivationReason): Promise<void> {
    const key = this.id.toString();
    scalingActivationCounts.set(key, (scalingActivationCounts.get(key) ?? 0) + 1);
    if (!scalingWaitingActivations.has(key)) scalingWaitingActivations.set(key, 0);
  }

  async wait(): Promise<void> {
    const key = this.id.toString();
    scalingWaitingActivations.set(key, (scalingWaitingActivations.get(key) ?? 0) + 1);
    await scalingSemaphore.wait();
    scalingWaitingActivations.set(key, (scalingWaitingActivations.get(key) ?? 0) - 1);
  }

  async release(): Promise<void> {
    scalingSemaphore.release();
  }

  async getActivationCount(): Promise<number> {
    return scalingActivationCounts.get(this.id.toString()) ?? 0;
  }

  async getWaitingCount(): Promise<number> {
    return scalingWaitingActivations.get(this.id.toString()) ?? 0;
  }
}

/** All activation guids ever seen for `StatelessWorkerGrain`, upstream's `static readonly HashSet<Guid>`. */
const allActivationIds = new Set<string>();

/**
 * Ported from `StatelessWorkerGrain`: `[StatelessWorker(MaxLocalWorkers: 1,
 * removeIdleWorkers: false)]`. `LongCall` holds its activation busy briefly
 * (upstream uses a 2-second interleaved grain timer to widen the race window
 * under a real threadpool; a short `setTimeout` reproduces the same
 * single-activation-under-concurrent-load scenario without slowing the
 * ported suite down).
 */
@grain({ name: "UnitTests.Grains.StatelessWorkerGrain", stateless: true, maxLocalWorkers: 1 })
export class StatelessWorkerGrain extends Grain implements IStatelessWorkerGrain {
  private activationGuid = "";
  private readonly calls: Array<[number, number]> = [];

  override async onActivate(_reason: ActivationReason): Promise<void> {
    this.activationGuid = Guid.newGuid().toString();
  }

  async longCall(): Promise<void> {
    allActivationIds.add(this.activationGuid);
    const start = Date.now();
    await new Promise<void>((resolve) => setTimeout(resolve, 20));
    this.calls.push([start, Date.now()]);
  }

  async getCallStats(): Promise<[string, string, Array<[number, number]>]> {
    const silo = this.runtime.localSiloAddress().toString();
    return [this.activationGuid, silo, [...this.calls]];
  }

  async dummyCall(): Promise<void> {}
}
