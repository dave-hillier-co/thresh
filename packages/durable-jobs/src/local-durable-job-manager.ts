import { durationToMs, type Duration } from "@thresh/core/duration";
import type {
  DurableJob,
  DurableJobsOptions,
  ScheduleJobRequest,
  ShouldRetry,
} from "@thresh/core/durable-job";
import { Guid } from "@thresh/core/guid";
import type { TimeProvider } from "@thresh/core/time-provider";
import { registerDurableJobQueueDepth } from "@thresh/observability/durable-job-metrics";
import { claimBudget, defaultShouldRetry, shardKeyFor } from "@thresh/durable-jobs/job-model";
import type { JobShardStore } from "@thresh/durable-jobs/job-shard-store";
import {
  ConcurrencyLimiter,
  ShardExecutor,
  type OverloadSignal,
  type RunJob,
} from "@thresh/durable-jobs/shard-executor";

export interface ResolvedDurableJobsOptions {
  shardDurationMs: number;
  shardActivationBufferMs: number;
  maxConcurrentJobsPerSilo: number;
  slowStartInitialConcurrency: number;
  slowStartGrowthFactor: number;
  slowStartIntervalMs: number;
  overloadBackoffMs: number;
  shouldRetry: ShouldRetry;
  maxAdoptedCount: number;
  claimRampUpBudget: number;
}

/** The membership facts the manager needs to claim and adopt shards. */
export interface ShardOwnershipContext {
  /** This silo's ring key (the owner identity recorded in the store). */
  localRingKey: string;
  /** Ring keys of the currently active silos; any other owner is treated as dead. */
  activeRingKeys: readonly string[];
}

/**
 * Per-silo durable-jobs service (Orleans' `LocalDurableJobManager`, a SystemTarget).
 * It buckets a scheduled job into its time shard, claims the shards it should own
 * from the store, and runs a `ShardExecutor` per owned shard. On a membership
 * change it reconciles ownership: claims newly orphaned / dead-silo shards (under
 * a ramp-up budget), drops shards it no longer owns. Clock-driven and
 * membership-reconciled; storage is the injected `JobShardStore`.
 */
export class LocalDurableJobManager {
  private readonly executors = new Map<number, ShardExecutor>();
  private readonly limiter: ConcurrencyLimiter;
  private ownership: ShardOwnershipContext;
  private readonly unregisterQueueDepth: () => void;

  constructor(
    private readonly store: JobShardStore,
    private readonly time: TimeProvider,
    private readonly runJob: RunJob,
    private readonly options: ResolvedDurableJobsOptions,
    ownership: ShardOwnershipContext,
    private readonly overload?: OverloadSignal,
    /**
     * Forward a job to the silo that owns its shard (Orleans mirrors this by
     * co-locating the SystemTarget on the owner; this port instead ships the
     * already-persisted job over a cluster message). Absent in single-silo
     * setups and tests, where every shard is always owned locally.
     */
    private readonly forwardJob?: (ownerRingKey: string, job: DurableJob) => Promise<boolean>,
  ) {
    this.limiter = new ConcurrencyLimiter(options.maxConcurrentJobsPerSilo);
    this.ownership = ownership;
    this.unregisterQueueDepth = registerDurableJobQueueDepth(() =>
      [...this.executors.values()].reduce((sum, executor) => sum + executor.size, 0),
    );
  }

  /** The shard keys this silo currently runs an executor for (for tests/metrics). */
  ownedShards(): number[] {
    return [...this.executors.keys()];
  }

  /**
   * Schedule a job: assign its shard, persist it, and — if this silo owns (or
   * claims) that shard — add it to the live executor so it fires at its due time.
   * If another (live) silo already owns the shard, forward the job to it so it
   * still gets enqueued somewhere rather than only sitting in the store until
   * its owner happens to reconcile. Returns the durable job (with its assigned
   * id and shard).
   */
  async scheduleJob(request: ScheduleJobRequest): Promise<DurableJob> {
    const shardKey = shardKeyFor(request.dueTime.getTime(), this.options.shardDurationMs);
    const job: DurableJob = {
      id: Guid.newGuid().toString(),
      name: request.name,
      dueTime: request.dueTime,
      target: request.target,
      shardKey,
      metadata: request.metadata ?? {},
    };
    await this.store.persistAdd(job);
    const executor = await this.ensureOwned(shardKey);
    if (executor !== undefined) {
      executor.enqueue(job);
    } else {
      await this.forwardToShardOwner(shardKey, job);
    }
    return job;
  }

  /**
   * Receive a job forwarded by another silo that scheduled it but did not own
   * the shard here (already persisted by the sender). Get it into this silo's
   * live executor for the shard, claiming the shard first if it is still
   * unclaimed. Returns whether it was accepted into a live executor here.
   */
  async receiveForwardedJob(job: DurableJob): Promise<boolean> {
    const executor = await this.ensureOwned(job.shardKey);
    executor?.enqueue(job);
    return executor !== undefined;
  }

  /** Look up the shard's current owner in the store and forward the job to it, if live. */
  private async forwardToShardOwner(shardKey: number, job: DurableJob): Promise<void> {
    if (this.forwardJob === undefined) return;
    const shards = await this.store.listShards();
    const owner = shards.find((s) => s.shardKey === shardKey)?.owner;
    if (owner === undefined || owner === this.ownership.localRingKey) return;
    await this.forwardJob(owner, job).catch(() => undefined);
  }

  /** Cancel a scheduled job (best-effort): remove it from its shard and the store. */
  async cancelJob(job: { id: string; shardKey: number }): Promise<void> {
    const executor = this.executors.get(job.shardKey);
    if (executor !== undefined) {
      await executor.cancel(job.id);
    } else {
      await this.store.persistRemove(job.shardKey, job.id).catch(() => undefined);
    }
  }

  /**
   * Reconcile shard ownership against the current membership view: drop executors
   * for shards no longer claimable here, then claim newly available shards
   * (orphaned or owned by a dead silo) under the ramp-up budget and start an
   * executor for each. Idempotent — already-owned shards keep their executors.
   */
  async refreshOwnership(ownership: ShardOwnershipContext): Promise<void> {
    this.ownership = ownership;
    const shards = await this.store.listShards();
    const active = new Set(ownership.activeRingKeys);

    // Resume our own shards after a restart: the store still records this ring key
    // as owner, but the process lost its in-memory executor, so start one (no
    // re-claim needed) to reload and fire the persisted jobs.
    for (const shard of shards) {
      if (shard.owner === ownership.localRingKey && !this.executors.has(shard.shardKey)) {
        await this.startExecutor(shard.shardKey);
      }
    }

    // Shards this silo can claim now: unclaimed, or owned by a dead silo (one not
    // in the active set), and not poisoned.
    const claimable = shards.filter(
      (s) =>
        !s.poisoned &&
        s.owner !== ownership.localRingKey &&
        (s.owner === undefined || !active.has(s.owner)),
    );
    const toClaim = claimBudget(claimable.length, this.options.claimRampUpBudget);
    for (const shard of claimable.slice(0, toClaim)) {
      // A dead-owner shard is adopted; an unclaimed one is just claimed.
      const deadOwner = shard.owner !== undefined && !active.has(shard.owner) ? [shard.owner] : [];
      await this.claimAndStart(shard.shardKey, deadOwner);
    }

    // Drop executors for shards the store no longer records as ours (claimed away).
    // Re-read so the just-made claims above are reflected, not the stale snapshot.
    const ownedNow = new Set(
      (await this.store.listShards())
        .filter((s) => s.owner === ownership.localRingKey)
        .map((s) => s.shardKey),
    );
    for (const shardKey of [...this.executors.keys()]) {
      if (!ownedNow.has(shardKey)) this.stopExecutor(shardKey);
    }
  }

  /** Stop every executor (silo shutdown). Best-effort release so a successor can claim. */
  async stop(): Promise<void> {
    for (const shardKey of [...this.executors.keys()]) {
      await this.store.releaseShard(shardKey, this.ownership.localRingKey).catch(() => undefined);
      this.stopExecutor(shardKey);
    }
    this.unregisterQueueDepth();
  }

  /** Ensure this silo owns the shard (claiming it if free) and has a running executor. */
  private async ensureOwned(shardKey: number): Promise<ShardExecutor | undefined> {
    const existing = this.executors.get(shardKey);
    if (existing !== undefined) return existing;
    return this.claimAndStart(shardKey, []);
  }

  private async claimAndStart(
    shardKey: number,
    deadOwners: readonly string[],
  ): Promise<ShardExecutor | undefined> {
    const claimed = await this.store.claimShard(shardKey, this.ownership.localRingKey, {
      adoptFromDead: deadOwners,
      maxAdoptedCount: this.options.maxAdoptedCount,
    });
    if (claimed === undefined) return undefined; // lost the claim or poisoned
    return this.startExecutor(shardKey);
  }

  private async startExecutor(shardKey: number): Promise<ShardExecutor> {
    const existing = this.executors.get(shardKey);
    if (existing !== undefined) return existing;
    const executor = new ShardExecutor(
      shardKey,
      this.store,
      this.time,
      this.limiter,
      this.runJob,
      this.overload ?? (() => this.limiter.saturated),
      {
        shardDurationMs: this.options.shardDurationMs,
        maxConcurrentJobsPerSilo: this.options.maxConcurrentJobsPerSilo,
        slowStartInitialConcurrency: this.options.slowStartInitialConcurrency,
        slowStartGrowthFactor: this.options.slowStartGrowthFactor,
        slowStartIntervalMs: this.options.slowStartIntervalMs,
        overloadBackoffMs: this.options.overloadBackoffMs,
        shouldRetry: this.options.shouldRetry,
      },
    );
    this.executors.set(shardKey, executor);
    await executor.load();
    return executor;
  }

  private stopExecutor(shardKey: number): void {
    const executor = this.executors.get(shardKey);
    if (executor === undefined) return;
    executor.stop();
    this.executors.delete(shardKey);
  }
}

const DAY = 86_400_000;

/** Resolve `DurableJobsOptions` defaults to plain milliseconds for the manager. */
export function resolveOptions(options: {
  shardDuration?: Duration;
  shardActivationBufferPeriod?: Duration;
  maxConcurrentJobsPerSilo?: number;
  slowStartInitialConcurrency?: number;
  slowStartGrowthFactor?: number;
  slowStartInterval?: Duration;
  overloadBackoff?: Duration;
  shouldRetry?: ShouldRetry;
  maxAdoptedCount?: number;
  claimRampUpBudget?: number;
}): ResolvedDurableJobsOptions {
  const shardDurationMs =
    options.shardDuration !== undefined ? durationToMs(options.shardDuration) : 3_600_000;
  return {
    shardDurationMs: shardDurationMs > 0 ? shardDurationMs : DAY,
    shardActivationBufferMs:
      options.shardActivationBufferPeriod !== undefined
        ? durationToMs(options.shardActivationBufferPeriod)
        : 60_000,
    maxConcurrentJobsPerSilo: options.maxConcurrentJobsPerSilo ?? 100,
    slowStartInitialConcurrency: options.slowStartInitialConcurrency ?? 1,
    slowStartGrowthFactor: options.slowStartGrowthFactor ?? 2,
    slowStartIntervalMs:
      options.slowStartInterval !== undefined ? durationToMs(options.slowStartInterval) : 10_000,
    overloadBackoffMs:
      options.overloadBackoff !== undefined ? durationToMs(options.overloadBackoff) : 1000,
    shouldRetry: options.shouldRetry ?? defaultShouldRetry,
    maxAdoptedCount: options.maxAdoptedCount ?? 3,
    claimRampUpBudget: options.claimRampUpBudget ?? 4,
  };
}

/**
 * The claim budget is unlimited: ramp-up is disabled (`rampUpDurationMs <= 0`)
 * or has fully elapsed. Orleans returns `int.MaxValue`; this port uses
 * `Number.MAX_SAFE_INTEGER` as the equivalent "no ceiling" sentinel.
 */
export const UNLIMITED_CLAIM_BUDGET = Number.MAX_SAFE_INTEGER;

/**
 * The time-based claim budget a freshly joined silo may spend on this
 * reconciliation step (Orleans `LocalDurableJobManager.ComputeClaimBudget`).
 * The budget ramps linearly from `initialBudget` to `maxBudget` over
 * `rampUpDurationMs`, becoming `UNLIMITED_CLAIM_BUDGET` once ramp-up is
 * disabled (`rampUpDurationMs <= 0`) or has elapsed. `totalClaimedShards`
 * already claimed this ramp-up window is subtracted from the interpolated
 * budget, floored at 0.
 *
 * NOTE: this is a pure, standalone function alongside the flat
 * `claimBudget` (job-model.ts) that `refreshOwnership` above actually calls.
 * TODO(parity): wire `computeClaimBudget` into `refreshOwnership`'s claim
 * path (tracking elapsed-since-join and cumulative-claimed-shards) so the
 * time-based ramp-up governs live reconciliation, not just this pure API.
 */
export function computeClaimBudget(
  rampUpDurationMs: number,
  initialBudget: number,
  maxBudget: number,
  elapsedMs: number,
  totalClaimedShards: number,
): number {
  if (rampUpDurationMs <= 0) return UNLIMITED_CLAIM_BUDGET;
  if (elapsedMs >= rampUpDurationMs) return UNLIMITED_CLAIM_BUDGET;
  const fraction = elapsedMs / rampUpDurationMs;
  const budget = initialBudget + Math.trunc(fraction * (maxBudget - initialBudget));
  return Math.max(0, budget - totalClaimedShards);
}

/** Raised when a `DurableJobsOptions` shard-claim-budget setting is invalid. */
export class DurableJobsConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DurableJobsConfigurationError";
  }
}

/**
 * Validates the shard-claim-budget options of `DurableJobsOptions` (Orleans
 * `DurableJobsOptionsValidator.ValidateConfiguration`): `shardClaimInitialBudget`
 * must not be negative, `shardClaimMaxBudget` must not be less than
 * `shardClaimInitialBudget`, and `shardClaimRampUpDuration` must not be
 * negative (zero is allowed — it disables ramp-up).
 */
export class DurableJobsOptionsValidator {
  constructor(private readonly options: DurableJobsOptions) {}

  validateConfiguration(): void {
    // Orleans' `DurableJobsOptions` defaults: ShardClaimInitialBudget = 2,
    // ShardClaimMaxBudget = 20, ShardClaimRampUpDuration = 5 minutes.
    const initialBudget = this.options.shardClaimInitialBudget ?? 2;
    const maxBudget = this.options.shardClaimMaxBudget ?? 20;
    const rampUpDurationMs =
      this.options.shardClaimRampUpDuration !== undefined
        ? durationToMs(this.options.shardClaimRampUpDuration)
        : 300_000;

    if (initialBudget < 0) {
      throw new DurableJobsConfigurationError(
        `ShardClaimInitialBudget must not be negative, but was ${initialBudget}.`,
      );
    }
    if (maxBudget < initialBudget) {
      throw new DurableJobsConfigurationError(
        `ShardClaimMaxBudget (${maxBudget}) must not be less than ShardClaimInitialBudget (${initialBudget}).`,
      );
    }
    if (rampUpDurationMs < 0) {
      throw new DurableJobsConfigurationError(
        `ShardClaimRampUpDuration must not be negative, but was ${rampUpDurationMs}ms.`,
      );
    }
  }
}
