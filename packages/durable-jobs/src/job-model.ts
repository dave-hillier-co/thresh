import { durationToMs, type Duration } from "@thresh/core/duration";
import type { ShouldRetry } from "@thresh/core/durable-job";

/**
 * The time-bucket shard key for a due time: `floor(dueTime / shardDuration)`
 * (Orleans). Jobs in the same bucket share a shard, owned by one silo at a time;
 * bucketing by *time* (not grain key) spreads a burst at one grain across silos.
 */
export function shardKeyFor(dueTimeMs: number, shardDurationMs: number): number {
  if (shardDurationMs <= 0) throw new Error("shardDuration must be positive");
  return Math.floor(dueTimeMs / shardDurationMs);
}

/** The wall-clock start of a shard bucket. */
export function shardStartMs(shardKey: number, shardDurationMs: number): number {
  return shardKey * shardDurationMs;
}

/**
 * The default retry policy (Orleans): retry a failed job up to 5 attempts total,
 * with exponential `2^n`s backoff between attempts (2s, 4s, 8s, 16s), then drop.
 * `dequeueCount` is the number of attempts already made (≥ 1).
 */
export const defaultShouldRetry: ShouldRetry = (dequeueCount) => {
  const maxAttempts = 5;
  if (dequeueCount >= maxAttempts) return undefined;
  return { seconds: 2 ** dequeueCount };
};

/**
 * The number of shards a freshly joined silo may claim in one ramp-up step
 * (Orleans' claim budget). A silo does not grab every orphaned shard at once on
 * join; it claims at most `budget` per step so a rejoining silo does not melt.
 * `pendingUnclaimed` is how many shards are available to claim; `budget` the
 * per-step ceiling. Returns how many to claim this step.
 */
export function claimBudget(pendingUnclaimed: number, budget: number): number {
  if (budget <= 0) return 0;
  return Math.min(Math.max(0, pendingUnclaimed), budget);
}

/** A job entry tracked in the in-memory due-time queue. */
export interface QueuedJob {
  id: string;
  /** Effective due time in ms (advanced by retry/poll backoff). */
  dueMs: number;
  /** How many times this job has been dequeued for execution (0 until first run). */
  dequeueCount: number;
  /** The opaque payload carried alongside the queue position. */
  readonly payload: unknown;
}

/**
 * An in-memory due-time priority queue — the working set of a shard (Orleans'
 * `InMemoryJobQueue`). Jobs are kept ordered by effective due time; `add`,
 * `cancel`, peeking the next-due job, dequeuing all jobs due by a cutoff, and
 * `retryLater` (re-enqueue at a later time, preserving the dequeue count) are the
 * operations the executor needs. Pure and clock-free: the caller passes "now".
 */
export class InMemoryJobQueue {
  private readonly jobs = new Map<string, QueuedJob>();
  /**
   * Ids known to this queue: added by `add`, cleared by `cancel`. Unlike
   * `jobs`, this is *not* cleared by `dequeueDue` — a job stays "live" while
   * it is out for execution (dequeued, not yet completed or retried), so
   * `retryLater` can still re-enqueue it. It distinguishes a legitimate retry
   * of an in-flight job from a stale/foreign/cancelled id.
   */
  private readonly liveIds = new Set<string>();

  get size(): number {
    return this.jobs.size;
  }

  has(id: string): boolean {
    return this.jobs.has(id);
  }

  get(id: string): QueuedJob | undefined {
    return this.jobs.get(id);
  }

  /** Add (or replace) a job at its due time. */
  add(job: QueuedJob): void {
    this.jobs.set(job.id, { ...job });
    this.liveIds.add(job.id);
  }

  /** Remove a job by id; returns whether it was present (a best-effort cancel). */
  cancel(id: string): boolean {
    this.liveIds.delete(id);
    return this.jobs.delete(id);
  }

  /** The earliest due time among queued jobs, or `undefined` if empty. */
  nextDueMs(): number | undefined {
    let earliest: number | undefined;
    for (const job of this.jobs.values()) {
      if (earliest === undefined || job.dueMs < earliest) earliest = job.dueMs;
    }
    return earliest;
  }

  /**
   * Remove and return every job due at or before `nowMs`, earliest first. The
   * `dequeueCount` is *not* touched here — the executor owns it: it counts run
   * attempts (a retry bumps it; a `pollAfter` re-poll, being a continuation of the
   * same attempt, does not), so incrementing per dequeue would over-count polls.
   */
  dequeueDue(nowMs: number): QueuedJob[] {
    const due = [...this.jobs.values()]
      .filter((j) => j.dueMs <= nowMs)
      .sort((a, b) => a.dueMs - b.dueMs);
    for (const job of due) this.jobs.delete(job.id);
    return due;
  }

  /**
   * Re-enqueue a previously dequeued job to run again after `delay` from `nowMs`,
   * with the `dequeueCount` the caller sets on `job`. Used for both retry (the
   * executor having bumped the count) and `pollAfter` (count preserved). A
   * no-op for an id this queue does not consider live (never added, or
   * explicitly cancelled) — it must not resurrect a dead id.
   */
  retryLater(job: QueuedJob, delay: Duration, nowMs: number): void {
    if (!this.liveIds.has(job.id)) return;
    this.jobs.set(job.id, { ...job, dueMs: nowMs + durationToMs(delay) });
  }
}
