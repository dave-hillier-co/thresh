import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKey } from "@tsva/core/grain-key";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { ReminderRegistry } from "@tsva/core/reminder";
import type { ActivationData } from "@tsva/runtime/activation";
import type { GrainFactory } from "@tsva/runtime/grain-factory";

/** Per-activation `GrainRuntime`, reached by a grain through `this.runtime`. */
export class GrainRuntimeImpl implements GrainRuntime {
  constructor(
    private readonly factory: GrainFactory,
    private readonly activation: ActivationData,
    private readonly reminders?: () => ReminderRegistry | undefined,
  ) {}

  getGrain<T>(def: GrainInterface<T>, key: GrainKey): T {
    return this.factory.getGrain(def, key);
  }

  registerTimer(callback: () => Promise<void>, due: Duration, period?: Duration): GrainTimer {
    return this.activation.registerTimer(callback, due, period);
  }

  registerReminder(name: string, due: Duration, period: Duration): Promise<void> {
    return this.requireReminders().register(this.activation.id, name, due, period);
  }

  unregisterReminder(name: string): Promise<void> {
    return this.requireReminders().unregister(this.activation.id, name);
  }

  deactivateOnIdle(): void {
    this.activation.requestDeactivation();
  }

  delayDeactivation(by: Duration): void {
    this.activation.delayDeactivation(durationToMs(by));
  }

  private requireReminders(): ReminderRegistry {
    const registry = this.reminders?.();
    if (registry === undefined) throw new Error("reminders are not configured on this silo");
    return registry;
  }
}
