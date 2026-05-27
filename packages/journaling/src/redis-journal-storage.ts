import type { createClient } from "redis";
import { InconsistentStateError } from "@tsva/core/errors";
import type { GrainId } from "@tsva/core/grain-id";
import type { JournalEntry, JournalSegment, JournalStorage } from "@tsva/core/journal-storage";

/** A connected node-redis client (the subset this provider drives). */
export type RedisClient = ReturnType<typeof createClient>;

export interface RedisJournalStorageOptions {
  /** Namespace for keys in the shared Redis (defaults to `"tsva"`). */
  keyPrefix?: string;
}

const CONFLICT = "TSVA_ETAG_CONFLICT";

// Conditional append: the caller's expected version (ARGV[1], '' = "no log yet")
// must match the stored version, then push the new entries (ARGV[2..]) and bump
// the version. Atomic on the server, so two silos racing to append produce one
// winner and one conflict — the optimistic-concurrency contract of GrainStorage,
// carried over to an append-only log.
const APPEND = `
local cur = redis.call('GET', KEYS[2])
if ARGV[1] == '' then
  if cur then return redis.error_reply('${CONFLICT} ' .. cur) end
elseif (not cur) or (cur ~= ARGV[1]) then
  return redis.error_reply('${CONFLICT} ' .. (cur or ''))
end
for i = 2, #ARGV do
  redis.call('RPUSH', KEYS[1], ARGV[i])
end
return redis.call('INCR', KEYS[2])`;

// Conditional snapshot replace: same version guard, then drop the whole log and
// write the snapshot frames in its place. Atomic, so a compaction racing a
// stale incarnation's append still leaves exactly one winner.
const REPLACE = `
local cur = redis.call('GET', KEYS[2])
if ARGV[1] == '' then
  if cur then return redis.error_reply('${CONFLICT} ' .. cur) end
elseif (not cur) or (cur ~= ARGV[1]) then
  return redis.error_reply('${CONFLICT} ' .. (cur or ''))
end
redis.call('DEL', KEYS[1])
for i = 2, #ARGV do
  redis.call('RPUSH', KEYS[1], ARGV[i])
end
return redis.call('INCR', KEYS[2])`;

/**
 * Durable, cluster-shared journal store backed by Redis. The log is a Redis list
 * of entry strings (`…:journal:{grain}/{log}`) alongside a version counter
 * (`…:journalver:{grain}/{log}`); appends and snapshot replaces run as
 * conditional Lua scripts so optimistic concurrency holds across silos. Reuses
 * the `RedisGrainStorage` Lua/CAS pattern and is interchangeable with
 * `MemoryJournalStorage`.
 */
export class RedisJournalStorage implements JournalStorage {
  private readonly keyPrefix: string;

  constructor(
    private readonly client: RedisClient,
    options: RedisJournalStorageOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "tsva";
  }

  async read(logName: string, grainId: GrainId): Promise<JournalSegment> {
    const versionRaw = await this.client.get(this.versionKey(logName, grainId));
    if (versionRaw === null) return { entries: [], version: undefined };
    const entries = await this.client.lRange(this.entriesKey(logName, grainId), 0, -1);
    return { entries, version: Number(versionRaw) };
  }

  async append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    return this.eval(APPEND, logName, grainId, entries, expectedVersion);
  }

  async replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    return this.eval(REPLACE, logName, grainId, entries, expectedVersion);
  }

  async clear(logName: string, grainId: GrainId): Promise<void> {
    await this.client.del([this.entriesKey(logName, grainId), this.versionKey(logName, grainId)]);
  }

  private async eval(
    script: string,
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    try {
      const result = await this.client.eval(script, {
        keys: [this.entriesKey(logName, grainId), this.versionKey(logName, grainId)],
        arguments: [expectedVersion === undefined ? "" : String(expectedVersion), ...entries],
      });
      return Number(result);
    } catch (err) {
      this.rethrowConflict(err, logName, grainId, expectedVersion);
    }
  }

  private rethrowConflict(
    err: unknown,
    logName: string,
    grainId: GrainId,
    expected: number | undefined,
  ): never {
    const message = err instanceof Error ? err.message : String(err);
    if (message.startsWith(CONFLICT)) {
      const stored = message.slice(CONFLICT.length).trim() || undefined;
      throw new InconsistentStateError(
        `journal version conflict on ${logName} for ${grainId.toString()}`,
        expected === undefined ? undefined : String(expected),
        stored,
      );
    }
    throw err;
  }

  private entriesKey(logName: string, grainId: GrainId): string {
    return `${this.keyPrefix}:journal:${grainId.toString()}/${logName}`;
  }

  private versionKey(logName: string, grainId: GrainId): string {
    return `${this.keyPrefix}:journalver:${grainId.toString()}/${logName}`;
  }
}
