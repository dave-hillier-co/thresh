import { durationToMs, type Duration } from "@thresh/core/duration";
import type {
  DurableJob,
  DurableJobRunResult,
  JobRunContext,
  ShouldRetry,
} from "@thresh/core/durable-job";
import { Guid } from "@thresh/core/guid";
import type { TimeProvider, TimerHandle } from "@thresh/core/time-provider";
import {
  recordJobCompleted,
  recordJobFailed,
  recordJobStarted,
} from "@thresh/observability/durable-job-metrics";
import { InMemoryJobQueue, type QueuedJob } from "@thresh/durable-jobs/job-model";
import type { JobShardStore } from "@thresh/durable-jobs/job-shard-store";

/** Delivers one attempt of a job to the target grain as a turn; resolves its result. */
export type RunJob = (job: JobRunContext) => Promise<DurableJobRunResult>;

/** Reports whether the silo is currently overloaded (concurrency saturated / unhealthy). */
export type OverloadSignal = () => boolean;

export interface ShardExecutorOptions {
  shardDurationMs: number;
  maxConcurrentJobsPerSilo: number;
  slowStartInitialConcurrency: number;
  slowStartGrowthFactor: number;
  /**
   * How often (in ms of elapsed wall-clock time since the shard started) the
   * slow-start ceiling is allowed to grow by `slowStartGrowthFactor` (Orleans'
   * `SlowStartInterval`). Gates the ramp on real elapsed time rather than on
   * poll cycles, so a large backlog cannot jump straight to full concurrency.
   */
  slowStartIntervalMs: number;
  overloadBackoffMs: number;
  shouldRetry: ShouldRetry;
}

/**
 * A concurrency limiter shared across a silo's shard executors — the silo-wide
 * cap on jobs running at once (Orleans' per-silo concurrency control). Slots are
 * acquired before a job runs and released when it settles.
 */
export class ConcurrencyLimiter {
  private inUse = 0;
  constructor(private readonly max: number) {}

  get saturated(): boolean {
    return this.max - this.inUse <= 0;
  }

  tryAcquire(): boolean {
    if (this.inUse >= this.max) return false;
    this.inUse += 1;
    return true;
  }

  release(): void {
    if (this.inUse > 0) this.inUse -= 1;
  }
}

/**
 * Consumes the due jobs of a single shard (Orleans' `ShardExecutor`). It holds the
 * shard's in-memory due-time queue (the working set), and on each poll cycle:
 *  - dequeues jobs due by now, up to the slow-start concurrency ceiling and the
 *    shared per-silo concurrency limiter (slow-start: a fresh executor starts
 *    small and grows the ceiling each clean cycle);
 *  - runs each as a turn on the target via `runJob`, applying the result —
 *    `completed` removes, `pollAfter` re-enqueues holding logic for a supervised
 *    re-poll, `failed`/throw applies the retry policy (backoff then drop);
 *  - backs off the poll loop when the silo is overloaded;
 *  - re-arms a timer for the next due job (`pollAfter` loop).
 * All durability goes through the injected `JobShardStore`.
 */
export class ShardExecutor {
  readonly queue = new InMemoryJobQueue();
  /** Payloads (the DurableJob) by id, so a dequeued job can be reconstructed for delivery. */
  private readonly jobs = new Map<string, DurableJob>();
  private concurrency: number;
  private pollHandle: TimerHandle | undefined;
  private stopped = false;
  private running = 0;
  /** In-flight `runOne` promises, so `stop()` can drain them before resolving. */
  private readonly inFlight = new Set<Promise<void>>();
  /** Wall-clock time (per the injected `TimeProvider`) this shard started ramping from. */
  private readonly startedAtMs: number;

  constructor(
    readonly shardKey: number,
    private readonly store: JobShardStore,
    private readonly time: TimeProvider,
    private readonly limiter: ConcurrencyLimiter,
    private readonly runJob: RunJob,
    private readonly isOverloaded: OverloadSignal,
    private readonly options: ShardExecutorOptions,
  ) {
    this.concurrency = Math.max(1, options.slowStartInitialConcurrency);
    this.startedAtMs = this.time.now();
  }

  /** Number of jobs in this shard's working set (queued, not yet completed). */
  get size(): number {
    return this.queue.size;
  }

  /** Load the shard's persisted jobs into the working set and arm the poll loop. */
  async load(): Promise<void> {
    for (const persisted of await this.store.readJobs(this.shardKey)) {
      // Idempotent fast-path: a tombstone entry (a prior RunId already marked
      // completed but whose `persistRemove` was lost) means the job ran. Skip
      // invocation and clean the entry — at-most-once for that RunId.
      if (persisted.completed === true) {
        await this.store.persistRemove(this.shardKey, persisted.job.id).catch(() => undefined);
        continue;
      }
      this.jobs.set(persisted.job.id, persisted.job);
      this.queue.add({
        id: persisted.job.id,
        dueMs: persisted.dueTime.getTime(),
        dequeueCount: persisted.dequeueCount,
        payload: persisted.job,
      });
    }
    this.schedulePoll();
  }

  /** Add a freshly scheduled job to the working set (already persisted by the manager). */
  enqueue(job: DurableJob): void {
    this.jobs.set(job.id, job);
    this.queue.add({
      id: job.id,
      dueMs: job.dueTime.getTime(),
      dequeueCount: 0,
      payload: job,
    });
    this.schedulePoll();
  }

  /** Cancel a pending job (best-effort): drop it from the queue and the store. */
  async cancel(jobId: string): Promise<boolean> {
    const removed = this.queue.cancel(jobId);
    this.jobs.delete(jobId);
    await this.store.persistRemove(this.shardKey, jobId).catch(() => undefined);
    return removed;
  }

  /**
   * Stop polling and await any run already in flight before resolving —
   * mirrors `QueuePullingAgent.stop()` (packages/streams/src/queue-pulling-agent.ts).
   * Without this, a rebalance or graceful shutdown could hand the shard to a
   * new owner while the old owner's handler is still executing, letting the
   * new owner re-run the same job concurrently.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    this.clearPoll();
    await Promise.all(this.inFlight);
  }

  /** Clear the poll timer if one is armed and reset the handle. */
  private clearPoll(): void {
    if (this.pollHandle !== undefined) {
      this.time.clearTimer(this.pollHandle);
      this.pollHandle = undefined;
    }
  }

  /** Re-arm the poll timer for the next due job (or immediately if one is already due). */
  private schedulePoll(): void {
    if (this.stopped) return;
    this.clearPoll();
    const next = this.queue.nextDueMs();
    if (next === undefined) return;
    const delay = Math.max(0, next - this.time.now());
    this.pollHandle = this.time.setTimer(() => {
      this.pollHandle = undefined;
      void this.poll();
    }, delay);
  }

  /**
   * The slow-start ceiling for the given elapsed time since the shard started:
   * `initial * growthFactor^n`, where `n` is the number of whole
   * `slowStartIntervalMs` periods that have elapsed, capped at the silo max.
   * Time-gated (Orleans' `SlowStartInterval`) so a backlog of due jobs cannot
   * ramp concurrency to the max within a handful of same-tick poll cycles —
   * the ceiling only grows once real wall-clock time has passed.
   */
  private slowStartCeiling(now: number): number {
    const {
      slowStartInitialConcurrency,
      slowStartGrowthFactor,
      slowStartIntervalMs,
      maxConcurrentJobsPerSilo,
    } = this.options;
    if (slowStartIntervalMs <= 0) return maxConcurrentJobsPerSilo;
    const elapsedIntervals = Math.max(
      0,
      Math.floor((now - this.startedAtMs) / slowStartIntervalMs),
    );
    const grown = slowStartInitialConcurrency * slowStartGrowthFactor ** elapsedIntervals;
    return Math.min(maxConcurrentJobsPerSilo, Math.max(1, Math.ceil(grown)));
  }

  /**
   * One poll cycle: when the silo is overloaded, back off and try again later
   * without running anything; otherwise dequeue the jobs due now (capped by the
   * slow-start ceiling and the shared limiter) and run each. The slow-start
   * ceiling itself only grows once enough wall-clock time has elapsed since
   * the shard started (see `slowStartCeiling`), then re-arms for the next due job.
   */
  private async poll(): Promise<void> {
    if (this.stopped) return;

    if (this.isOverloaded()) {
      this.pollHandle = this.time.setTimer(() => {
        this.pollHandle = undefined;
        void this.poll();
      }, this.options.overloadBackoffMs);
      return;
    }

    const now = this.time.now();
    // Slow-start: the ceiling only rises with elapsed wall-clock time, never on
    // its own regresses (Math.max guards against clock skew between reads).
    this.concurrency = Math.max(this.concurrency, this.slowStartCeiling(now));
    const due = this.queue.dequeueDue(now);
    const settled: Array<Promise<void>> = [];
    let admitted = 0;
    let blocked = false;
    for (const job of due) {
      // Respect both the per-shard slow-start ceiling — checked against jobs
      // actually in flight for this shard (`this.running`, incremented
      // synchronously by `runOne` before its first await, so it already counts
      // this cycle's admissions) — and the silo-wide limiter.
      if (this.running >= this.concurrency || !this.limiter.tryAcquire()) {
        // Could not run now: re-enqueue at the same due time to retry.
        this.queue.add({ ...job, dueMs: now });
        blocked = true;
        continue;
      }
      admitted += 1;
      const run = this.runOne(job, now);
      this.inFlight.add(run);
      void run.finally(() => this.inFlight.delete(run));
      settled.push(run);
    }

    // Re-arm before awaiting in-flight runs so a job due during this cycle is
    // picked up. If we admitted nothing yet still have due jobs (saturated by
    // the slow-start ceiling or the shared limiter), a 0-delay re-poll — the
    // re-enqueued jobs are due *now* — would busy-loop under a real clock until
    // an in-flight job completes (which re-polls via `runOne`'s `finally`) or
    // the ceiling grows. Back off to the next ceiling-growth boundary instead of
    // spinning; a completion still re-polls immediately on the fast path.
    if (admitted === 0 && blocked) {
      this.scheduleSaturatedPoll(now);
    } else {
      this.schedulePoll();
    }
    await Promise.all(settled);
  }

  /**
   * Re-arm the poll after a cycle that could admit nothing because the shard is
   * saturated. Waits until the slow-start ceiling can next grow (so the ramp
   * makes progress on real elapsed time), or a bounded fallback when the ceiling
   * is already at the silo cap — in both cases an in-flight job completing
   * re-polls sooner via `runOne`, so this only bounds the idle-wait, never the
   * throughput.
   */
  private scheduleSaturatedPoll(now: number): void {
    if (this.stopped) return;
    this.clearPoll();
    const { slowStartIntervalMs, maxConcurrentJobsPerSilo, overloadBackoffMs } = this.options;
    let delay: number;
    if (this.concurrency < maxConcurrentJobsPerSilo && slowStartIntervalMs > 0) {
      const elapsedIntervals = Math.max(
        0,
        Math.floor((now - this.startedAtMs) / slowStartIntervalMs),
      );
      const nextBoundaryMs = this.startedAtMs + (elapsedIntervals + 1) * slowStartIntervalMs;
      delay = Math.max(1, nextBoundaryMs - now);
    } else {
      delay = Math.max(1, overloadBackoffMs);
    }
    this.pollHandle = this.time.setTimer(() => {
      this.pollHandle = undefined;
      void this.poll();
    }, delay);
  }

  /** Run one dequeued job, apply its result, and release the concurrency slot. */
  private async runOne(job: QueuedJob, now: number): Promise<void> {
    this.running += 1;
    const durable = this.jobs.get(job.id);
    try {
      if (durable === undefined) return; // cancelled between dequeue and run
      // `dequeueCount` carried on the queued job is the number of attempts already
      // made; this run is the next attempt (≥ 1).
      const attempt = job.dequeueCount + 1;
      // Assign a fresh RunId per claimed attempt and persist it alongside the
      // durable claim before invoking the handler. If this silo crashes mid-
      // flight and another claims the shard, the next claimer sees the RunId
      // marker; once we mark it completed below, a lost `persistRemove` is
      // recoverable — the next claimer's `load()` will skip and clean up.
      const runId = Guid.newGuid().toString();
      await this.store.persistRunStart(this.shardKey, durable.id, runId).catch(() => undefined);
      recordJobStarted({ "thresh.job.name": durable.name });
      const context: JobRunContext = {
        id: durable.id,
        name: durable.name,
        dueTime: durable.dueTime,
        target: durable.target,
        metadata: durable.metadata,
        dequeueCount: attempt,
        runId,
      };
      let result: DurableJobRunResult;
      try {
        result = await this.runJob(context);
      } catch (error) {
        result = { kind: "failed", error };
      }
      await this.applyResult(job, result, attempt, now, runId);
    } finally {
      this.running -= 1;
      this.limiter.release();
      this.schedulePoll();
    }
  }

  /** Apply a job's run result: complete (remove), poll-after (re-enqueue), or retry/drop. */
  private async applyResult(
    job: QueuedJob,
    result: DurableJobRunResult,
    attempt: number,
    now: number,
    runId: string,
  ): Promise<void> {
    if (result.kind === "completed") {
      // Mark the RunId completed *before* removing, so a crash between the two
      // leaves a tombstone the next claimer can detect and skip (per-RunId
      // dedup against double-invocation on shard rebalance).
      await this.store.persistRunComplete(this.shardKey, job.id, runId).catch(() => undefined);
      this.jobs.delete(job.id);
      await this.store.persistRemove(this.shardKey, job.id).catch(() => undefined);
      recordJobCompleted({ "thresh.job.id": job.id });
      return;
    }
    if (result.kind === "pollAfter") {
      // A supervised re-poll is a continuation of the same attempt: the dequeue
      // count is unchanged, so the next poll sees the same attempt number.
      this.reschedule({ ...job, dequeueCount: job.dequeueCount }, result.delay, now);
      return;
    }
    // failed: consult the retry policy with the number of attempts made so far.
    recordJobFailed({ "thresh.job.id": job.id });
    const backoff = this.options.shouldRetry(attempt, result.error);
    if (backoff === undefined) {
      this.jobs.delete(job.id);
      await this.store.persistRemove(this.shardKey, job.id).catch(() => undefined);
      return;
    }
    // This run counts as a made attempt: bump so the next run is attempt + 1.
    this.reschedule({ ...job, dequeueCount: attempt }, backoff, now);
  }

  /** Re-enqueue a job after `delay`, persisting its new due time and dequeue count. */
  private reschedule(job: QueuedJob, delay: Duration, now: number): void {
    const dueMs = now + durationToMs(delay);
    this.queue.retryLater(job, delay, now);
    this.schedulePoll();
    void this.store
      .persistRetry(this.shardKey, job.id, new Date(dueMs), job.dequeueCount)
      .catch(() => undefined);
  }
}
