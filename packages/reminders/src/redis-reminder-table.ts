import { randomUUID } from "node:crypto";
import type { createClient } from "redis";
import type { GrainId } from "@thresh/core/grain-id";
import type { ReminderEntry, ReminderRegistration, ReminderTable } from "@thresh/core/reminder";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import type { ReminderData } from "./reminder-data";

export type RedisClient = ReturnType<typeof createClient>;

export interface RedisReminderTableOptions {
  /** Namespace for keys in the shared Redis (defaults to `"thresh"`). */
  keyPrefix?: string;
  /**
   * Logical service identity this provider's keys belong to (Orleans'
   * `ClusterOptions.ServiceId`). Two clusters pointed at the same Redis stay
   * partitioned by it. Defaults to `"default"`, Orleans' own
   * `ClusterOptions.DefaultServiceId`; `SiloBuilder` threads the silo's
   * `serviceId ?? DEFAULT_SERVICE_ID`.
   *
   * Redis has no ALTER: a key written before this option existed has no
   * `{serviceId}` segment and is invisible to a service-partitioned reader —
   * a deliberate upgrade break, the same call #59 made for `RedisGrainStorage`
   * (see todo.md / docs/deviations.md).
   */
  serviceId?: string;
}

// Upsert is unconditional (a fresh etag each time, like MemoryReminderTable):
// write the entry hash, index it by hash for range ownership, and record the
// name under its grain. Atomic so the three structures never diverge.
const UPSERT = `
redis.call('HSET', KEYS[1], 'data', ARGV[3], 'etag', ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[5])
return ARGV[4]`;

// Remove only if the caller's etag still matches; clears all three structures.
const REMOVE = `
local cur = redis.call('HGET', KEYS[1], 'etag')
if not cur or cur ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[3])
return 1`;

/**
 * Durable, cluster-shared reminder table backed by Redis, mirroring Orleans'
 * Redis reminder table. Each reminder is a Redis hash; a sorted set indexes them
 * by uniform hash code so `readRange` (hash-range ownership) is a server-side
 * range query, and a per-grain set backs `readForGrain`. Interchangeable with
 * `MemoryReminderTable`.
 */
export class RedisReminderTable implements ReminderTable {
  private readonly keyPrefix: string;
  private readonly serviceId: string;

  constructor(
    private readonly client: RedisClient,
    options: RedisReminderTableOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "thresh";
    this.serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
  }

  async upsert(registration: ReminderRegistration): Promise<string> {
    const { grainId, name } = registration;
    const etag = randomUUID();
    const data: ReminderData = {
      grainId,
      name,
      startAt: registration.startAt,
      period: registration.period,
    };
    await this.client.eval(UPSERT, {
      keys: [this.entryKey(grainId, name), this.indexKey(), this.grainKey(grainId)],
      arguments: [
        this.member(grainId, name),
        String(grainId.getUniformHashCode()),
        serializeValue(data),
        etag,
        name,
      ],
    });
    return etag;
  }

  async remove(grainId: GrainId, name: string, etag: string): Promise<boolean> {
    const result = await this.client.eval(REMOVE, {
      keys: [this.entryKey(grainId, name), this.indexKey(), this.grainKey(grainId)],
      arguments: [this.member(grainId, name), etag, name],
    });
    return result === 1;
  }

  async read(grainId: GrainId, name: string): Promise<ReminderEntry | undefined> {
    return this.entryAt(this.entryKey(grainId, name));
  }

  async readForGrain(grainId: GrainId): Promise<ReminderEntry[]> {
    const names = await this.client.sMembers(this.grainKey(grainId));
    return this.entriesAt(names.map((n) => this.entryKey(grainId, n)));
  }

  async readRange(hashBegin: number, hashEnd: number): Promise<ReminderEntry[]> {
    const members =
      hashBegin <= hashEnd
        ? // Half-open [begin, end): begin inclusive, end exclusive.
          await this.client.zRangeByScore(this.indexKey(), hashBegin, `(${hashEnd}`)
        : // Wrap-around: [begin, +inf] ∪ [-inf, end).
          [
            ...(await this.client.zRangeByScore(this.indexKey(), hashBegin, "+inf")),
            ...(await this.client.zRangeByScore(this.indexKey(), "-inf", `(${hashEnd}`)),
          ];
    return this.entriesAt(members.map((m) => this.memberEntryKey(m)));
  }

  private async entriesAt(keys: string[]): Promise<ReminderEntry[]> {
    const entries = await Promise.all(keys.map((k) => this.entryAt(k)));
    return entries.filter((e): e is ReminderEntry => e !== undefined);
  }

  private async entryAt(key: string): Promise<ReminderEntry | undefined> {
    const record = await this.client.hGetAll(key);
    if (record.etag === undefined) return undefined;
    const data = deserializeValue<ReminderData>(record.data ?? "null");
    return { ...data, etag: record.etag };
  }

  private member(grainId: GrainId, name: string): string {
    return `${grainId.toString()}${name}`;
  }

  private indexKey(): string {
    return `${this.keyPrefix}:${this.serviceId}:rem:index`;
  }

  private grainKey(grainId: GrainId): string {
    return `${this.keyPrefix}:${this.serviceId}:rem:g:${grainId.toString()}`;
  }

  private entryKey(grainId: GrainId, name: string): string {
    return this.memberEntryKey(this.member(grainId, name));
  }

  private memberEntryKey(member: string): string {
    return `${this.keyPrefix}:${this.serviceId}:rem:e:${member}`;
  }
}
