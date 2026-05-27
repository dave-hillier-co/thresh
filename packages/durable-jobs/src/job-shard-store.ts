import type { DurableJob } from "@tsva/core/durable-job";

/**
 * The durable record of a shard's ownership and bookkeeping (Orleans' shard
 * metadata). A shard is owned by one silo at a time; `adoptedCount` counts how
 * many times it has been adopted from a dead owner (a shard adopted past
 * `maxAdoptedCount` is poisoned and no longer claimed).
 */
export interface ShardRecord {
  shardKey: number;
  /** Ring key of the owning silo, or `undefined` if unclaimed/orphaned. */
  owner: string | undefined;
  /** Times this shard has been adopted from a dead owner. */
  adoptedCount: number;
  /** True once `adoptedCount` exceeds the limit — the shard is abandoned. */
  poisoned: boolean;
}

/**
 * The pluggable durable store for durable jobs, mirroring Orleans' `IJobShard`
 * persist hooks plus the shard-ownership table. It is the *only* storage contact:
 * the in-memory due-time queue is the working set, and these hooks make the queue
 * durable. Memory (dev/test) and Redis (default) implement it.
 *
 * Jobs persist under their shard so a silo that claims a shard re-reads its jobs;
 * the persist hooks (`persistAdd` / `persistRemove` / `persistRetry`) keep the
 * store in step with the in-memory queue.
 */
export interface JobShardStore {
  // ── job persistence (the IJobShard persist hooks) ──

  /** Persist a newly scheduled job under its shard. */
  persistAdd(job: DurableJob): Promise<void>;
  /** Persist the removal of a completed or cancelled job. */
  persistRemove(shardKey: number, jobId: string): Promise<void>;
  /** Persist a job's new due time and dequeue count after a retry / re-poll. */
  persistRetry(shardKey: number, jobId: string, dueTime: Date, dequeueCount: number): Promise<void>;
  /** Read every job currently persisted under a shard (on claim / restart). */
  readJobs(shardKey: number): Promise<PersistedJob[]>;

  // ── shard ownership ──

  /** The shard records that have at least one persisted job (for ownership reconciliation). */
  listShards(): Promise<ShardRecord[]>;
  /**
   * Atomically claim a shard for `owner`: succeeds if the shard is unclaimed, or
   * (when `adoptFromDead` lists dead silo ring keys) currently owned by a dead
   * silo — in which case `adoptedCount` is incremented. A poisoned shard is never
   * claimable. Returns the resulting record on success, or `undefined` if the
   * claim lost (another silo owns it, or it is poisoned).
   */
  claimShard(
    shardKey: number,
    owner: string,
    options: { adoptFromDead?: readonly string[]; maxAdoptedCount: number },
  ): Promise<ShardRecord | undefined>;
  /** Release a shard this silo owns (graceful drain), so a successor can claim it. */
  releaseShard(shardKey: number, owner: string): Promise<void>;
}

/** A job as read back from the store, carrying its current dequeue count. */
export interface PersistedJob {
  job: DurableJob;
  /** How many times the job has been dequeued for execution (0 if never run). */
  dequeueCount: number;
  /** The current effective due time (advanced by retry / re-poll backoff). */
  dueTime: Date;
}
