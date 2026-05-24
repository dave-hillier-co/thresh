import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKey } from "@tsva/core/grain-key";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { ActivationData } from "@tsva/runtime/activation";
import type { GrainFactory } from "@tsva/runtime/grain-factory";

/** Per-activation `GrainRuntime`, reached by a grain through `this.runtime`. */
export class GrainRuntimeImpl implements GrainRuntime {
  constructor(
    private readonly factory: GrainFactory,
    private readonly activation: ActivationData,
  ) {}

  getGrain<T>(def: GrainInterface<T>, key: GrainKey): T {
    return this.factory.getGrain(def, key);
  }

  registerTimer(callback: () => Promise<void>, due: Duration, period?: Duration): GrainTimer {
    return this.activation.registerTimer(callback, due, period);
  }

  deactivateOnIdle(): void {
    this.activation.requestDeactivation();
  }

  delayDeactivation(by: Duration): void {
    this.activation.delayDeactivation(durationToMs(by));
  }
}
