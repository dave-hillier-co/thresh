import type { Duration } from "./duration";
import type { GrainInterface } from "./grain-interface";
import type { GrainTimer } from "./grain-timer";
import type { GrainKeyFor } from "./key-kinds";
import type { SiloAddress } from "./silo-address";
import type { BroadcastChannelProvider } from "./broadcast-channel";
import type { DurableJob, ScheduleJobRequest } from "./durable-job";
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
}
