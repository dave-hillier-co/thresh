import type { Duration } from "./duration";
import type { GrainInterface } from "./grain-interface";
import type { GrainTimer, TimerOptions } from "./grain-timer";
import type { GrainKeyFor } from "./key-kinds";
import type { SiloAddress } from "./silo-address";
import type { BroadcastChannelProvider } from "./broadcast-channel";
import type { DurableJob, ScheduleJobRequest } from "./durable-job";
import type { GrainReminder } from "./reminder";
import type { StreamProvider } from "./stream";

/**
 * The runtime services a grain reaches through `this.runtime`, resolved per
 * activation.
 */
export interface GrainRuntime {
  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T;
  registerTimer(
    callback: () => Promise<void>,
    due: Duration,
    period?: Duration,
    options?: TimerOptions,
  ): GrainTimer;
  registerReminder(name: string, due: Duration, period: Duration): Promise<void>;
  unregisterReminder(name: string): Promise<void>;
  /** Look up a reminder registered to this grain (Orleans `IReminderRegistry.GetReminder`). */
  getReminder(name: string): Promise<GrainReminder | undefined>;
  /** All reminders registered to this grain (Orleans `IReminderRegistry.GetReminders`). */
  getReminders(): Promise<GrainReminder[]>;
  /**
   * Schedule a durable job: one invocation of a target grain's
   * `DURABLE_JOB_HANDLER` at a due time, made durable, retried and failed-over
   * Returns the durable job; pass it to {@link cancelJob} to cancel.
   */
  scheduleJob(request: ScheduleJobRequest): Promise<DurableJob>;
  /** Best-effort cancel of a scheduled job that has not yet completed. */
  cancelJob(job: DurableJob): Promise<void>;
  getStreamProvider(name?: string): StreamProvider;
  /** The named broadcast-channel provider (Orleans `IBroadcastChannelProvider`). */
  getBroadcastChannelProvider(name?: string): BroadcastChannelProvider;
  deactivateOnIdle(): void;
  delayDeactivation(by: Duration): void;
  /**
   * Request that this activation migrate to another silo the next time it goes
   * idle, carrying its in-memory state (gathered from `IGrainMigrationParticipant`s)
   * rather than being deactivated and forgotten. Pass `targetSilo` to direct the
   * move at a specific silo; otherwise the grain's placement strategy chooses.
   */
  migrateOnIdle(targetSilo?: SiloAddress): void;
  /**
   * The `SiloAddress` of the silo currently hosting this activation (Orleans'
   * `IGrainContext.Address` / `GetRuntimeInstanceId()`), so a grain can report
   * where it is placed.
   */
  localSiloAddress(): SiloAddress;
  /**
   * Read a value from the ambient request-context bag for this turn (Orleans
   * `RequestContext.Get`). Sourced from the caller's request and any values a
   * call filter wrote into it; `undefined` if the key was never set.
   */
  getRequestContext(key: string): string | undefined;
  /**
   * Write a value into the ambient request-context bag for this turn (Orleans
   * `RequestContext.Set`). Flows to any downstream grain call made during the
   * same turn; does not affect the caller's own context.
   */
  setRequestContext(key: string, value: string): void;
  /**
   * The ambient cancellation signal for this turn, or `undefined` if none
   * applies (Orleans has no analogue — JS-only cooperative cancellation; see
   * `docs/deviations.md`). Composed from whatever of these are in play: the
   * caller's `InvokeCallOptions.signal`/per-call deadline
   * (`@tsva/runtime/dispatcher`) and any `GrainCancellationToken` bound
   * during this call. Aborting it does NOT stop an already-started turn — JS
   * has no thread interruption — so this is for grain code that wants to
   * observe cancellation cooperatively mid-turn (e.g. pass it to a
   * cancellable `fetch`); a still-queued turn is preempted automatically
   * (`TurnScheduler`).
   */
  getCancellationSignal(): AbortSignal | undefined;
  /**
   * The id of the transaction this turn runs inside, or `undefined` outside
   * any transaction (Orleans `TransactionContext.GetTransactionInfo()?.Id`).
   * Whether one is present is decided by the invoked method's
   * `TransactionOption` (see `InvokeMethodOptions.transaction`), not by this
   * accessor — it only reports the outcome of that boundary resolution.
   */
  getTransactionId(): string | undefined;
  /** Whether this turn runs inside a transaction, ambient or just begun. */
  isInTransaction(): boolean;
  /**
   * Detach a call from the ambient transaction's own completion without
   * awaiting it (Orleans `TransactionContext.GetRequiredTransactionInfo()
   * .Fork()`). Marks the transaction as having an outstanding "orphaned" call
   * so its root boundary aborts rather than commits if that call is never
   * matched by a completion — see `TransactionOrphanCallError`
   * (`@tsva/core/errors`). Throws if no transaction is ambient.
   */
  forkTransaction(): void;
  /**
   * Get this activation's bound instance of a `GrainExtension` interface
   * (Orleans `IGrainExtension`/`IGrainContext.GetGrainExtension`), lazily
   * creating and binding one via `factory` on first use. Idempotent: once
   * bound, later calls return the SAME instance and never re-run `factory`.
   * The extension object becomes the dispatch target for calls made against
   * `iface` (a grain reference cast to `iface` via `castGrainReference`),
   * routed by the activation instead of the grain instance's own methods.
   */
  getOrSetExtension<T extends object>(iface: GrainInterface<T>, factory: () => T): T;
  /**
   * Toggle this activation's HOSTING SILO's overload detection on or off
   * (Orleans test-hook `IPlacementTestGrain.EnableOverloadDetection`, backed
   * by `OverloadDetector.Enabled`). Silo-scoped, not activation-scoped: any
   * grain hosted here can flip it.
   */
  enableOverloadDetection(enabled: boolean): void;
  /**
   * Force this activation's hosting silo's reported CPU usage to `value`
   * until `unlatchCpuUsage()` (Orleans test-hook `IPlacementTestGrain.LatchCpuUsage`,
   * backed by `TestHooksEnvironmentStatisticsProvider.LatchHardwareStatistics`).
   */
  latchCpuUsage(value: number): void;
  /** Clear a CPU-usage latch set by `latchCpuUsage` (Orleans `UnlatchCpuUsage`). */
  unlatchCpuUsage(): void;
  /**
   * Force this activation's hosting silo into the overloaded state (Orleans
   * test-hook `IPlacementTestGrain.LatchOverloaded`): equivalent to latching
   * CPU usage just above `LoadSheddingOptions.cpuThreshold`.
   */
  latchOverloaded(): void;
  /** Clear an overload latch set by `latchOverloaded` (Orleans `UnlatchOverloaded`). */
  unlatchOverloaded(): void;
  /**
   * Ping a specific silo's control target (Orleans `ISiloControl.Ping`): a
   * health-check no-op that completes when the target silo is reachable.
   * Used for silo-level diagnostics. Resolves once the target acknowledges;
   * rejects if the silo is unreachable.
   */
  pingSilo(siloAddress: SiloAddress, message?: string): Promise<void>;
}
