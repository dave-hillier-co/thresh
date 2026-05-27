import type { createClient } from "redis";
import type { DurableJob } from "@tsva/core/durable-job";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";
import type { JobShardStore, PersistedJob, ShardRecord } from "@tsva/durable-jobs/job-shard-store";

export type RedisClient = ReturnType<typeof createClient>;

export interface RedisJobShardStoreOptions {
  /** Namespace for keys in the shared Redis (defaults to `"tsva"`). */
  keyPrefix?: string;
}

/** The codec-serialized payload of a persisted job (its current due time + dequeue count). */
interface JobData {
  job: DurableJob;
  dequeueCount: number;
  dueTime: Date;
}

// Claim a shard for a new owner. Succeeds if the shard is unclaimed (no owner) or
// the current owner is in the dead set; a poisoned shard is never claimed. When
// adopting from a dead owner, bump adoptedCount and poison past the limit. The
// shard hash holds `owner`, `adoptedCount`, `poisoned`. KEYS[1] is the shard hash;
// ARGV: owner, maxAdoptedCount, deadJSON.
const CLAIM = `
local owner = redis.call('HGET', KEYS[1], 'owner')
local adopted = tonumber(redis.call('HGET', KEYS[1], 'adoptedCount') or '0')
local poisoned = redis.call('HGET', KEYS[1], 'poisoned')
if poisoned == '1' then return nil end
if owner == ARGV[1] then
  return cjson.encode({ owner = owner, adoptedCount = adopted, poisoned = false })
end
local dead = {}
for _, k in ipairs(cjson.decode(ARGV[3])) do dead[k] = true end
local unowned = (owner == false or owner == nil)
local ownerDead = (owner ~= false and owner ~= nil and dead[owner] == true)
if not unowned and not ownerDead then return nil end
if ownerDead then
  adopted = adopted + 1
  if adopted > tonumber(ARGV[2]) then
    redis.call('HSET', KEYS[1], 'poisoned', '1', 'adoptedCount', adopted)
    redis.call('HDEL', KEYS[1], 'owner')
    return nil
  end
end
redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'adoptedCount', adopted, 'poisoned', '0')
return cjson.encode({ owner = ARGV[1], adoptedCount = adopted, poisoned = false })`;

// Release a shard only if this caller still owns it. KEYS[1] shard hash; ARGV owner.
const RELEASE = `
if redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('HDEL', KEYS[1], 'owner')
end
return 1`;

/**
 * Durable, cluster-shared `JobShardStore` backed by Redis (the default store).
 * Jobs are persisted in a per-shard hash keyed by job id; a set tracks which
 * shards have jobs; each shard's ownership/adoption bookkeeping lives in its own
 * hash, claimed atomically with a Lua CAS (mirrors `RedisReminderTable`). The
 * working-set due-time queue lives in the executor; this store is the durability
 * seam (the `IJobShard` persist hooks). Interchangeable with `MemoryJobShardStore`.
 */
export class RedisJobShardStore implements JobShardStore {
  private readonly keyPrefix: string;

  constructor(
    private readonly client: RedisClient,
    options: RedisJobShardStoreOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "tsva";
  }

  async persistAdd(job: DurableJob): Promise<void> {
    const data: JobData = { job, dequeueCount: 0, dueTime: job.dueTime };
    await this.client
      .multi()
      .hSet(this.jobsKey(job.shardKey), job.id, serializeValue(data))
      .sAdd(this.shardsKey(), String(job.shardKey))
      .exec();
  }

  async persistRemove(shardKey: number, jobId: string): Promise<void> {
    await this.client.hDel(this.jobsKey(shardKey), jobId);
  }

  async persistRetry(
    shardKey: number,
    jobId: string,
    dueTime: Date,
    dequeueCount: number,
  ): Promise<void> {
    const raw = await this.client.hGet(this.jobsKey(shardKey), jobId);
    if (raw == null) return;
    const data = deserializeValue<JobData>(raw);
    const next: JobData = { job: data.job, dequeueCount, dueTime };
    await this.client.hSet(this.jobsKey(shardKey), jobId, serializeValue(next));
  }

  async readJobs(shardKey: number): Promise<PersistedJob[]> {
    const record = await this.client.hGetAll(this.jobsKey(shardKey));
    return Object.values(record).map((raw) => {
      const data = deserializeValue<JobData>(raw);
      return { job: data.job, dequeueCount: data.dequeueCount, dueTime: data.dueTime };
    });
  }

  async listShards(): Promise<ShardRecord[]> {
    const keys = await this.client.sMembers(this.shardsKey());
    const records = await Promise.all(
      keys.map(async (k) => {
        const shardKey = Number(k);
        const count = await this.client.hLen(this.jobsKey(shardKey));
        if (count === 0) return undefined; // no jobs: skip (and drop the stale index entry)
        const meta = await this.client.hGetAll(this.shardMetaKey(shardKey));
        return {
          shardKey,
          owner: meta.owner ?? undefined,
          adoptedCount: meta.adoptedCount !== undefined ? Number(meta.adoptedCount) : 0,
          poisoned: meta.poisoned === "1",
        } satisfies ShardRecord;
      }),
    );
    return records.filter((r): r is ShardRecord => r !== undefined);
  }

  async claimShard(
    shardKey: number,
    owner: string,
    options: { adoptFromDead?: readonly string[]; maxAdoptedCount: number },
  ): Promise<ShardRecord | undefined> {
    const result = (await this.client.eval(CLAIM, {
      keys: [this.shardMetaKey(shardKey)],
      arguments: [
        owner,
        String(options.maxAdoptedCount),
        JSON.stringify([...(options.adoptFromDead ?? [])]),
      ],
    })) as string | null;
    if (result == null) return undefined;
    const parsed = JSON.parse(result) as { owner: string; adoptedCount: number; poisoned: boolean };
    return { shardKey, owner: parsed.owner, adoptedCount: parsed.adoptedCount, poisoned: false };
  }

  async releaseShard(shardKey: number, owner: string): Promise<void> {
    await this.client.eval(RELEASE, { keys: [this.shardMetaKey(shardKey)], arguments: [owner] });
  }

  private shardsKey(): string {
    return `${this.keyPrefix}:job:shards`;
  }

  private jobsKey(shardKey: number): string {
    return `${this.keyPrefix}:job:s:${shardKey}`;
  }

  private shardMetaKey(shardKey: number): string {
    return `${this.keyPrefix}:job:m:${shardKey}`;
  }
}
