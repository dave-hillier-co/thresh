import type { DurableJob } from "@thresh/core/durable-job";
import {
  type JobShardStore,
  type PersistedJob,
  type ShardRecord,
  toPersistedJob,
} from "@thresh/durable-jobs/job-shard-store";

interface Entry {
  job: DurableJob;
  dequeueCount: number;
  dueTime: Date;
  runId?: string;
  completed?: boolean;
}

interface ShardState {
  record: ShardRecord;
  jobs: Map<string, Entry>;
}

/**
 * In-memory `JobShardStore` for development and tests. A single instance shared
 * across silo restarts (a fresh node, same store) stands in for a durable backend
 * — exactly how `MemoryJournalStorage` is used in the journaling restart tests.
 */
export class MemoryJobShardStore implements JobShardStore {
  private readonly shards = new Map<number, ShardState>();

  private shard(shardKey: number): ShardState {
    let state = this.shards.get(shardKey);
    if (state === undefined) {
      state = {
        record: { shardKey, owner: undefined, adoptedCount: 0, poisoned: false },
        jobs: new Map(),
      };
      this.shards.set(shardKey, state);
    }
    return state;
  }

  async persistAdd(job: DurableJob): Promise<void> {
    this.shard(job.shardKey).jobs.set(job.id, { job, dequeueCount: 0, dueTime: job.dueTime });
  }

  async persistRemove(shardKey: number, jobId: string): Promise<void> {
    this.shards.get(shardKey)?.jobs.delete(jobId);
  }

  async persistRetry(
    shardKey: number,
    jobId: string,
    dueTime: Date,
    dequeueCount: number,
  ): Promise<void> {
    const entry = this.shards.get(shardKey)?.jobs.get(jobId);
    if (entry !== undefined) {
      entry.dueTime = dueTime;
      entry.dequeueCount = dequeueCount;
    }
  }

  async persistRunStart(shardKey: number, jobId: string, runId: string): Promise<void> {
    const entry = this.shards.get(shardKey)?.jobs.get(jobId);
    if (entry !== undefined) {
      entry.runId = runId;
      entry.completed = false;
    }
  }

  async persistRunComplete(shardKey: number, jobId: string, runId: string): Promise<void> {
    const entry = this.shards.get(shardKey)?.jobs.get(jobId);
    if (entry !== undefined && entry.runId === runId) {
      entry.completed = true;
    }
  }

  async readJobs(shardKey: number): Promise<PersistedJob[]> {
    const state = this.shards.get(shardKey);
    if (state === undefined) return [];
    return [...state.jobs.values()].map((e) => toPersistedJob(e));
  }

  async listShards(): Promise<ShardRecord[]> {
    return [...this.shards.values()].filter((s) => s.jobs.size > 0).map((s) => ({ ...s.record }));
  }

  async claimShard(
    shardKey: number,
    owner: string,
    options: { adoptFromDead?: readonly string[]; maxAdoptedCount: number },
  ): Promise<ShardRecord | undefined> {
    const state = this.shard(shardKey);
    const record = state.record;
    if (record.poisoned) return undefined;
    if (record.owner === owner) return { ...record };

    const dead = new Set(options.adoptFromDead ?? []);
    const unowned = record.owner === undefined;
    const ownerDead = record.owner !== undefined && dead.has(record.owner);
    if (!unowned && !ownerDead) return undefined; // a live silo owns it

    if (ownerDead) {
      record.adoptedCount += 1;
      if (record.adoptedCount > options.maxAdoptedCount) {
        record.poisoned = true;
        record.owner = undefined;
        return undefined;
      }
    }
    record.owner = owner;
    return { ...record };
  }

  async releaseShard(shardKey: number, owner: string): Promise<void> {
    const state = this.shards.get(shardKey);
    if (state !== undefined && state.record.owner === owner) state.record.owner = undefined;
  }
}
