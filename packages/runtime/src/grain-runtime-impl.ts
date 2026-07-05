import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKey } from "@tsva/core/grain-key";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { GrainReminder, ReminderEntry, ReminderRegistry } from "@tsva/core/reminder";
import type { DurableJob, DurableJobScheduler, ScheduleJobRequest } from "@tsva/core/durable-job";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { BroadcastChannelProvider } from "@tsva/core/broadcast-channel";
import { isActivationBound, type StreamProvider } from "@tsva/core/stream";
import type { ActivationData } from "@tsva/runtime/activation";
import { ActivationStreamProvider } from "@tsva/runtime/activation-stream-provider";
import type { GrainFactory } from "@tsva/runtime/grain-factory";
import { currentTransaction, requestContext } from "@tsva/runtime/invocation-context";

export interface GrainRuntimeServices {
  reminders?: () => ReminderRegistry | undefined;
  streams?: (name?: string) => StreamProvider | undefined;
  broadcastChannels?: (name?: string) => BroadcastChannelProvider | undefined;
  durableJobs?: () => DurableJobScheduler | undefined;
  /** Resolves this silo's own address, for `GrainRuntime.localSiloAddress()`. */
  localSilo?: () => SiloAddress | undefined;
}

/** Per-activation `GrainRuntime`, reached by a grain through `this.runtime`. */
export class GrainRuntimeImpl implements GrainRuntime {
  constructor(
    private readonly factory: GrainFactory,
    private readonly activation: ActivationData,
    private readonly services: GrainRuntimeServices = {},
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

  async getReminder(name: string): Promise<GrainReminder | undefined> {
    const entry = await this.requireReminders().getReminder(this.activation.id, name);
    return entry === undefined ? undefined : toGrainReminder(entry);
  }

  async getReminders(): Promise<GrainReminder[]> {
    const entries = await this.requireReminders().getReminders(this.activation.id);
    return entries.map(toGrainReminder);
  }

  scheduleJob(request: ScheduleJobRequest): Promise<DurableJob> {
    return this.requireDurableJobs().scheduleJob(request);
  }

  cancelJob(job: DurableJob): Promise<void> {
    return this.requireDurableJobs().cancelJob(job);
  }

  getStreamProvider(name?: string): StreamProvider {
    const base = this.services.streams?.(name);
    if (base === undefined) throw new Error("streams are not configured on this silo");
    // Pulling-agent providers deliver through the dispatcher to a handler bound on
    // this activation; the memory provider instead wraps each onNext as a turn.
    if (isActivationBound(base)) {
      return base.bindActivation({
        grainId: this.activation.id,
        setHandler: (key, handler) => this.activation.setStreamHandler(key, handler),
        clearHandler: (key) => this.activation.clearStreamHandler(key),
      });
    }
    return new ActivationStreamProvider(
      base,
      (cb) => this.activation.runStreamTurn(cb),
      this.activation.id.toString(),
    );
  }

  getBroadcastChannelProvider(name?: string): BroadcastChannelProvider {
    const provider = this.services.broadcastChannels?.(name);
    if (provider === undefined)
      throw new Error("broadcast channels are not configured on this silo");
    return provider;
  }

  deactivateOnIdle(): void {
    this.activation.requestDeactivation();
  }

  migrateOnIdle(targetSilo?: SiloAddress): void {
    this.activation.requestMigration(targetSilo);
  }

  localSiloAddress(): SiloAddress {
    const silo = this.services.localSilo?.();
    if (silo === undefined) throw new Error("local silo address is not configured on this runtime");
    return silo;
  }

  delayDeactivation(by: Duration): void {
    this.activation.delayDeactivation(durationToMs(by));
  }

  getRequestContext(key: string): string | undefined {
    return requestContext.get(key);
  }

  setRequestContext(key: string, value: string): void {
    requestContext.set(key, value);
  }

  getTransactionId(): string | undefined {
    return currentTransaction()?.id;
  }

  isInTransaction(): boolean {
    return currentTransaction() !== undefined;
  }

  private requireReminders(): ReminderRegistry {
    const registry = this.services.reminders?.();
    if (registry === undefined) throw new Error("reminders are not configured on this silo");
    return registry;
  }

  private requireDurableJobs(): DurableJobScheduler {
    const scheduler = this.services.durableJobs?.();
    if (scheduler === undefined) throw new Error("durable jobs are not configured on this silo");
    return scheduler;
  }
}

/** Projects a durable `ReminderEntry` to the grain-facing shape, dropping `grainId`/`etag`. */
function toGrainReminder(entry: ReminderEntry): GrainReminder {
  return { name: entry.name, startAt: entry.startAt, period: entry.period };
}
