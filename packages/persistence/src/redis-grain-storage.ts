import { randomUUID } from "node:crypto";
import type { createClient } from "redis";
import { InconsistentStateError } from "@tsva/core/errors";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainStorage, StateHolder } from "@tsva/core/grain-storage";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";

/** A connected node-redis client (the subset this provider drives). */
export type RedisClient = ReturnType<typeof createClient>;

export interface RedisGrainStorageOptions {
  /** Namespace for keys in the shared Redis (defaults to `"tsva"`). */
  keyPrefix?: string;
}

const CONFLICT = "TSVA_ETAG_CONFLICT";

// Conditional write: only set when the caller's expected etag matches the
// stored one (or no record exists yet). Atomic on the server so two silos
// racing to write the same grain produce one winner and one conflict — the
// same optimistic-concurrency contract as MemoryGrainStorage.
const WRITE = `
local cur = redis.call('HGET', KEYS[1], 'etag')
if cur then
  if ARGV[1] == '' or ARGV[1] ~= cur then
    return redis.error_reply('${CONFLICT} ' .. cur)
  end
end
redis.call('HSET', KEYS[1], 'etag', ARGV[3], 'data', ARGV[2])
return ARGV[3]`;

// Conditional delete with the same etag guard; a missing record is a no-op.
const CLEAR = `
local cur = redis.call('HGET', KEYS[1], 'etag')
if cur then
  if ARGV[1] == '' or ARGV[1] ~= cur then
    return redis.error_reply('${CONFLICT} ' .. cur)
  end
  redis.call('DEL', KEYS[1])
end
return 'OK'`;

/**
 * Durable, cluster-shared storage provider backed by Redis. Each named state is
 * a Redis hash holding the serialized `value` and its `etag`; writes and clears
 * are conditional Lua scripts so optimistic concurrency holds across silos.
 * Mirrors Orleans' Redis grain storage and is interchangeable with
 * `MemoryGrainStorage`.
 */
export class RedisGrainStorage implements GrainStorage {
  private readonly keyPrefix: string;

  constructor(
    private readonly client: RedisClient,
    options: RedisGrainStorageOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "tsva";
  }

  async read<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    const record = await this.clientFor(signal).hGetAll(this.key(stateName, grainId));
    if (record.etag === undefined) {
      state.exists = false;
      state.etag = undefined;
      return;
    }
    state.value = deserializeValue<T>(record.data ?? "null");
    state.etag = record.etag;
    state.exists = true;
  }

  async write<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    const etag = randomUUID();
    try {
      await this.clientFor(signal).eval(WRITE, {
        keys: [this.key(stateName, grainId)],
        arguments: [state.etag ?? "", serializeValue(state.value), etag],
      });
    } catch (err) {
      this.rethrowConflict(err, stateName, grainId, state.etag);
    }
    state.etag = etag;
    state.exists = true;
  }

  async clear<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    try {
      await this.clientFor(signal).eval(CLEAR, {
        keys: [this.key(stateName, grainId)],
        arguments: [state.etag ?? ""],
      });
    } catch (err) {
      this.rethrowConflict(err, stateName, grainId, state.etag);
    }
    state.etag = undefined;
    state.exists = false;
  }

  /** The client to issue this call on: scoped to `signal` (node-redis's own abort hook) when given. */
  private clientFor(signal: AbortSignal | undefined): RedisClient {
    return signal === undefined ? this.client : this.client.withAbortSignal(signal);
  }

  private rethrowConflict(
    err: unknown,
    stateName: string,
    grainId: GrainId,
    expected: string | undefined,
  ): never {
    const message = err instanceof Error ? err.message : String(err);
    if (message.startsWith(CONFLICT)) {
      const stored = message.slice(CONFLICT.length).trim() || undefined;
      throw new InconsistentStateError(
        `etag conflict writing ${stateName} for ${grainId.toString()}`,
        expected,
        stored,
      );
    }
    throw err;
  }

  private key(stateName: string, grainId: GrainId): string {
    return `${this.keyPrefix}:state:${grainId.toString()}/${stateName}`;
  }
}
