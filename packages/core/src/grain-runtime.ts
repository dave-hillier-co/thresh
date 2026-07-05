import type { Duration } from "./duration";
import type { GrainInterface } from "./grain-interface";
import type { GrainTimer } from "./grain-timer";
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
  registerTimer(callback: () => Promise<void>, due: Duration, period?: Duration): GrainTimer;
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
   * The id of the transaction this turn runs inside, or `undefined` outside
   * any transaction (Orleans `TransactionContext.GetTransactionInfo()?.Id`).
   * Whether one is present is decided by the invoked method's
   * `TransactionOption` (see `InvokeMethodOptions.transaction`), not by this
   * accessor — it only reports the outcome of that boundary resolution.
   */
  getTransactionId(): string | undefined;
  /** Whether this turn runs inside a transaction, ambient or just begun. */
  isInTransaction(): boolean;
}
