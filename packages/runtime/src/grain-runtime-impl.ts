import { durationToMs, type Duration } from "@thresh/core/duration";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey, GrainKeyKind } from "@thresh/core/grain-key";
import type { GrainRuntime } from "@thresh/core/grain-runtime";
import type { GrainTimer, TimerOptions } from "@thresh/core/grain-timer";
import type { GrainReminder, ReminderEntry, ReminderRegistry } from "@thresh/core/reminder";
import type { DurableJob, DurableJobScheduler, ScheduleJobRequest } from "@thresh/core/durable-job";
import type { SiloAddress } from "@thresh/core/silo-address";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import type { BroadcastChannelProvider } from "@thresh/core/broadcast-channel";
import { isActivationBound, type StreamProvider } from "@thresh/core/stream";
import { forkTransaction } from "@thresh/core/transaction-info";
import type { ActivationData } from "@thresh/runtime/activation";
import { ActivationStreamProvider } from "@thresh/runtime/activation-stream-provider";
import type { GrainFactory } from "@thresh/runtime/grain-factory";
import {
  currentSignal,
  currentTransaction,
  requestContext,
  requireTransaction,
} from "@thresh/runtime/invocation-context";
import type { SiloLoadSheddingTestHooks } from "@thresh/runtime/load-shedding";
import type { GrainServiceRegistry } from "@thresh/runtime/grain-service";

export interface GrainRuntimeServices {
  /**
   * The silo's configured clock, surfaced to grain code as
   * `GrainRuntime.timeProvider`. The catalog always wires the silo's own
   * `TimeProvider` here; it is optional only so a test can construct a
   * `GrainRuntimeImpl` directly, and falls back to `systemTimeProvider`.
   */
  time?: TimeProvider;
  reminders?: () => ReminderRegistry | undefined;
  streams?: (name?: string) => StreamProvider | undefined;
  broadcastChannels?: (name?: string) => BroadcastChannelProvider | undefined;
  durableJobs?: () => DurableJobScheduler | undefined;
  /** Resolves this silo's own address, for `GrainRuntime.localSiloAddress()`. */
  localSilo?: () => SiloAddress | undefined;
  /** Resolves this silo's load-shedding test hooks, for `GrainRuntime.latchCpuUsage()`-style methods. */
  loadShedding?: () => SiloLoadSheddingTestHooks | undefined;
  /** Pings a specific silo's control target, for `GrainRuntime.pingSilo()`. */
  siloPing?: (siloAddress: SiloAddress, message?: string) => Promise<void>;
  /** Resolves this silo's grain-service registry, for `GrainRuntime.getGrainService()`. */
  grainServices?: () => GrainServiceRegistry | undefined;
  /**
   * Whether this activation's grain type is a `[StatelessWorker]` (Orleans
   * `ActivationData.IsStatelessWorker`). A stateless-worker id may have several
   * concurrent activations, so nothing here can bind a resource to "the"
   * activation — `getStreamProvider` consults this to reject a subscription
   * attempt the same way Orleans' `SiloStreamProviderRuntime.BindExtension`
   * does (`InvalidOperationException`).
   */
  isStatelessWorker?: () => boolean;
}

/** Per-activation `GrainRuntime`, reached by a grain through `this.runtime`. */
export class GrainRuntimeImpl implements GrainRuntime {
  constructor(
    private readonly factory: GrainFactory,
    private readonly activation: ActivationData,
    private readonly services: GrainRuntimeServices = {},
  ) {}

  /**
   * The silo's configured clock (Orleans `IGrainRuntime.TimeProvider`). The
   * SAME provider `registerTimer` schedules against, so a grain's own
   * time-based state advances with a `FakeTimeProvider` in tests.
   */
  get timeProvider(): TimeProvider {
    return this.services.time ?? systemTimeProvider;
  }

  /**
   * Widens the key to the runtime `GrainKey` union: the declared kind narrows
   * the caller-facing `GrainRuntime.getGrain`, not this implementation seam.
   */
  getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: GrainKey): T {
    return this.factory.getGrain(def, key);
  }

  registerTimer(
    callback: () => Promise<void>,
    due: Duration,
    period?: Duration,
    options?: TimerOptions,
  ): GrainTimer {
    return this.activation.registerTimer(callback, due, period, options);
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
    if (this.services.isStatelessWorker?.() === true) {
      throw new Error(
        "A stream provider cannot be used from a [StatelessWorker] grain: a stream " +
          "subscription must bind to a single activation, but a stateless worker may have " +
          "several concurrent ones.",
      );
    }
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
      // onNext runs through the incoming call-filter pipeline (Orleans parity:
      // a grain's own filter wraps its stream deliveries); onError/onCompleted
      // stay plain turns.
      (cb) => this.activation.runStreamDelivery(cb),
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

  getCancellationSignal(): AbortSignal | undefined {
    return currentSignal();
  }

  getTransactionId(): string | undefined {
    return currentTransaction()?.id;
  }

  isInTransaction(): boolean {
    return currentTransaction() !== undefined;
  }

  forkTransaction(): void {
    forkTransaction(requireTransaction());
  }

  getOrSetExtension<T extends object>(iface: GrainInterface<T>, factory: () => T): T {
    return this.activation.getOrSetExtension(iface.id, factory);
  }

  enableOverloadDetection(enabled: boolean): void {
    this.requireLoadShedding().enableOverloadDetection(enabled);
  }

  latchCpuUsage(value: number): Promise<void> {
    return this.requireLoadShedding().latchCpuUsage(value);
  }

  unlatchCpuUsage(): Promise<void> {
    return this.requireLoadShedding().unlatchCpuUsage();
  }

  latchOverloaded(): Promise<void> {
    return this.requireLoadShedding().latchOverloaded();
  }

  unlatchOverloaded(): Promise<void> {
    return this.requireLoadShedding().unlatchOverloaded();
  }

  pingSilo(siloAddress: SiloAddress, message?: string): Promise<void> {
    const ping = this.services.siloPing;
    if (ping === undefined) throw new Error("silo ping is not configured on this runtime");
    return ping(siloAddress, message);
  }

  getGrainService<T>(name: string): T {
    const registry = this.services.grainServices?.();
    if (registry === undefined) throw new Error("grain services are not configured on this silo");
    // `GrainServiceRegistry.get` is constrained to `GrainService` (its actual
    // stored type); `GrainRuntime.getGrainService`'s `T` is unconstrained
    // (core has no dependency on `@thresh/runtime`), so the cast bridges that —
    // same shape as `getGrain`'s reliance on the caller supplying the right `T`.
    return registry.get(name) as unknown as T;
  }

  private requireLoadShedding(): SiloLoadSheddingTestHooks {
    const hooks = this.services.loadShedding?.();
    if (hooks === undefined) throw new Error("load shedding is not configured on this runtime");
    return hooks;
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
