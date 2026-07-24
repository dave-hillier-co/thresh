import type { Duration } from "./duration";
import { defineGrainInterface, type GrainInterface } from "./grain-interface";
import type { GrainId } from "./grain-id";

/**
 * A durable job: one scheduled invocation of a target grain at a due time, made
 * durable, sharded by due time, retried and failed-over. Despite the
 * name it is not a workflow — it is "a reminder at scale, one-shot, with retries,
 * concurrency control and crash-failover". Mirrors Orleans' `DurableJob`.
 */
export interface DurableJob {
  /** Globally unique id of this job (a Guid string). */
  id: string;
  /** The handler name the target grain dispatches on (Orleans' job `Name`). */
  name: string;
  /** When the job becomes due to run. */
  dueTime: Date;
  /** The grain whose `DURABLE_JOB_HANDLER` runs the job, as a turn on its activation. */
  target: GrainId;
  /** The time-bucket shard key, `floor(dueTime / shardDuration)` (single-writer at shard granularity). */
  shardKey: number;
  /** Opaque, codec-serializable payload handed to the handler. */
  metadata: Readonly<Record<string, unknown>>;
}

/** The arguments to schedule a job, from any grain via `runtime.scheduleJob`. */
export interface ScheduleJobRequest {
  target: GrainId;
  name: string;
  /** Absolute time the job becomes due. */
  dueTime: Date;
  metadata?: Readonly<Record<string, unknown>>;
}

/**
 * The job handed to a `DURABLE_JOB_HANDLER` for one attempt. `dequeueCount` is the
 * number of times this job has been dequeued for execution (starts at 1 on the
 * first attempt); handlers must be idempotent (at-least-once delivery).
 */
export interface JobRunContext {
  id: string;
  name: string;
  dueTime: Date;
  target: GrainId;
  metadata: Readonly<Record<string, unknown>>;
  /** How many times this job has been dequeued for execution (≥ 1). */
  dequeueCount: number;
  /**
   * A fresh per-attempt identifier (Orleans' `RunId`), assigned by the shard
   * executor when a job is claimed for execution and persisted alongside the
   * durable claim. It lets a re-claim after a mid-flight rebalance detect that
   * a prior run already completed and skip invocation (idempotent fast-path).
   */
  runId: string;
}

/**
 * The outcome of running a job (Orleans `DurableJobRunResult`):
 * - `completed` — done, remove the job from the shard.
 * - `pollAfter` — not done yet; re-poll after the delay, holding the concurrency
 *   slot (a supervised long-running job, not a retry).
 * - `failed` — the attempt failed; the retry policy decides whether to retry
 *   (with backoff) or drop. Throwing from a handler is equivalent to `failed`.
 */
export type DurableJobRunResult =
  | { kind: "completed" }
  | { kind: "pollAfter"; delay: Duration }
  | { kind: "failed"; error: unknown };

export const completed: DurableJobRunResult = { kind: "completed" };

export function pollAfter(delay: Duration): DurableJobRunResult {
  return { kind: "pollAfter", delay };
}

export function failed(error: unknown): DurableJobRunResult {
  return { kind: "failed", error };
}

/** The grain's job handler: resolves a result for one attempt of a job. */
export type DurableJobHandler = (job: JobRunContext) => Promise<DurableJobRunResult>;

/**
 * A target grain opts into receiving durable jobs by exposing, under this symbol,
 * the handler that runs each due job as a turn on its activation. The runtime
 * resolves it lazily on the first delivered job. Mirrors the symbol-observer idiom
 * (`BROADCAST_CHANNEL_OBSERVER` / `STREAM_SUBSCRIPTION_OBSERVER`); the symbol key
 * keeps it from colliding with the grain's own (string-named) methods.
 */
export const DURABLE_JOB_HANDLER = Symbol.for("tsva.durableJobHandler");

/** A grain that handles durable jobs delivered to it. */
export interface DurableJobReceiver {
  [DURABLE_JOB_HANDLER]: DurableJobHandler;
}

/**
 * The runtime-facing handle a grain's `scheduleJob` / `cancelJob` delegate to.
 * The `LocalDurableJobManager` implements it; the hosting layer injects it into
 * the runtime (parity with `ReminderRegistry`).
 */
export interface DurableJobScheduler {
  scheduleJob(request: ScheduleJobRequest): Promise<DurableJob>;
  cancelJob(job: { id: string; shardKey: number }): Promise<void>;
  /**
   * Receive a job forwarded by another silo that scheduled it but does not own
   * (or could not claim) its time shard here. The job is already persisted by
   * the sender; this only needs to get it into this silo's live executor for
   * the shard, claiming the shard first if unclaimed. Returns whether the job
   * was accepted into a live executor here. Cross-silo forwarding only — not
   * called by grain code.
   */
  receiveForwardedJob?(job: DurableJob): Promise<boolean>;
}

/** The grain's durable-job handler, bound to it, or `undefined` if it declares none. */
export function durableJobHandler(instance: object): DurableJobHandler | undefined {
  const fn = (instance as Record<symbol, unknown>)[DURABLE_JOB_HANDLER];
  return typeof fn === "function"
    ? (job) => (fn as DurableJobHandler).call(instance, job)
    : undefined;
}

/**
 * System extension the shard executor invokes (through the dispatcher) to run one
 * attempt of a job on the target grain's single activation — so the job runs as a
 * turn, reactivating an idle grain wherever it is placed. The runtime intercepts
 * it and runs the grain's `DURABLE_JOB_HANDLER`; the grain never declares this
 * method. Mirrors the `BroadcastConsumer` / reminder delivery path.
 */
export interface DurableJobConsumer {
  runJob(job: JobRunContext): Promise<DurableJobRunResult>;
}

export const DurableJobConsumerInterface: GrainInterface<DurableJobConsumer> =
  defineGrainInterface<DurableJobConsumer>("system.DurableJobConsumer");

/**
 * Decides whether a failed job is retried. Returns the delay before the next
 * attempt, or `undefined` to drop the job (Orleans' `ShouldRetry`). `dequeueCount`
 * is the number of attempts so far (≥ 1).
 */
export type ShouldRetry = (dequeueCount: number, error: unknown) => Duration | undefined;

/** Tunables for the durable-jobs subsystem (mirrors Orleans `DurableJobsOptions`). */
export interface DurableJobsOptions {
  /** Width of a time shard; jobs bucket by `floor(dueTime / shardDuration)`. Default 1h. */
  shardDuration?: Duration;
  /** How early a shard is activated before its first job is due. Default 1m. */
  shardActivationBufferPeriod?: Duration;
  /** Max jobs running concurrently across all shards on one silo. Default 100. */
  maxConcurrentJobsPerSilo?: number;
  /** Concurrency a fresh executor starts at before ramping to the max (slow-start). Default 1. */
  slowStartInitialConcurrency?: number;
  /** Multiplier applied to the slow-start concurrency on each successful poll cycle. Default 2. */
  slowStartGrowthFactor?: number;
  /** Backoff applied to a shard's poll loop when the silo is overloaded. Default 1s. */
  overloadBackoff?: Duration;
  /** Retry policy for a failed job; default ≤ 5 attempts with `2^n`s backoff. */
  shouldRetry?: ShouldRetry;
  /** A shard adopted from a dead owner more than this many times is poisoned. Default 3. */
  maxAdoptedCount?: number;
  /** Shards a freshly joined silo may claim per ramp-up step (claim budget). Default 4. */
  claimRampUpBudget?: number;
  /**
   * The claim budget a freshly joined silo starts at, before ramping to
   * `shardClaimMaxBudget` over `shardClaimRampUpDuration` (Orleans
   * `DurableJobsOptions.ShardClaimInitialBudget`). Must be >= 0.
   */
  shardClaimInitialBudget?: number;
  /**
   * The claim budget a silo ramps up to once `shardClaimRampUpDuration` has
   * elapsed (Orleans `DurableJobsOptions.ShardClaimMaxBudget`). Must be >=
   * `shardClaimInitialBudget`.
   */
  shardClaimMaxBudget?: number;
  /**
   * How long a freshly joined silo takes to ramp its claim budget from
   * `shardClaimInitialBudget` to `shardClaimMaxBudget` (Orleans
   * `DurableJobsOptions.ShardClaimRampUpDuration`). Zero disables ramp-up
   * (unlimited budget immediately); must not be negative.
   */
  shardClaimRampUpDuration?: Duration;
}
