import { randomUUID } from "node:crypto";
import { InconsistentStateError } from "@tsva/core/errors";
import type { GrainId } from "@tsva/core/grain-id";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@tsva/core/transactional-storage";
import {
  applyStore,
  emptyRecord,
  type StoredRecord,
} from "@tsva/transactions/transactional-storage-apply";

/** A connected node-redis client (the subset this provider drives). */
type RedisClient = {
  hGetAll(key: string): Promise<Record<string, string>>;
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
};

export interface RedisTransactionalStorageOptions {
  /** Namespace for keys in the shared Redis (defaults to `"tsva"`). */
  keyPrefix?: string;
}

const CONFLICT = "TSVA_TX_ETAG_CONFLICT";

// Conditional write of the whole record: set only when the caller's expected
// etag matches the stored one (or no record exists). Atomic on the server, so a
// stale incarnation racing a successor produces one winner and one conflict.
const WRITE = `
local cur = redis.call('HGET', KEYS[1], 'etag')
if cur then
  if ARGV[1] == '' or ARGV[1] ~= cur then
    return redis.error_reply('${CONFLICT} ' .. cur)
  end
end
redis.call('HSET', KEYS[1], 'etag', ARGV[3], 'data', ARGV[2])
return ARGV[3]`;

/**
 * Durable, cluster-shared transactional storage backed by Redis. Each named
 * state is a Redis hash holding the JSON-encoded record (committed version,
 * pending states, commit-records metadata) and its `etag`. A `store` reads the
 * record, applies its deltas ({@link applyStore} — identical to the in-memory
 * provider), then writes back under a conditional Lua script, so the
 * optimistic-concurrency contract holds across silos. Single-activation
 * placement means only one writer touches a given record at a time; the etag
 * guards against a stale incarnation.
 */
export class RedisTransactionalStorage implements TransactionalStateStorage {
  private readonly keyPrefix: string;

  constructor(
    private readonly client: RedisClient,
    options: RedisTransactionalStorageOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "tsva";
  }

  async load(
    stateName: string,
    grainId: GrainId,
  ): Promise<TransactionalStorageLoadResponse<unknown>> {
    const hash = await this.client.hGetAll(this.key(stateName, grainId));
    if (hash.etag === undefined) {
      return {
        etag: undefined,
        committedState: undefined,
        committedSequenceId: 0,
        metadata: { timeStamp: 0, commitRecords: {} },
        pendingStates: [],
      };
    }
    const record = JSON.parse(hash.data ?? "{}") as StoredRecord<unknown>;
    return {
      etag: hash.etag,
      committedState: record.committedState,
      committedSequenceId: record.committedSequenceId,
      metadata: record.metadata,
      pendingStates: record.pendingStates,
    };
  }

  async store(
    stateName: string,
    grainId: GrainId,
    expectedETag: string | undefined,
    metadata: TransactionalStateMetadata,
    statesToPrepare: ReadonlyArray<PendingTransactionState<unknown>>,
    commitUpTo: number | undefined,
    abortAfter: number | undefined,
  ): Promise<string> {
    const key = this.key(stateName, grainId);
    const hash = await this.client.hGetAll(key);
    const record =
      hash.data !== undefined ? (JSON.parse(hash.data) as StoredRecord<unknown>) : emptyRecord();
    applyStore(record, metadata, statesToPrepare, commitUpTo, abortAfter);

    const etag = randomUUID();
    try {
      await this.client.eval(WRITE, {
        keys: [key],
        arguments: [expectedETag ?? "", JSON.stringify(record), etag],
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (message.startsWith(CONFLICT)) {
        const stored = message.slice(CONFLICT.length).trim() || undefined;
        throw new InconsistentStateError(
          `transactional store etag conflict for ${key}`,
          expectedETag,
          stored,
        );
      }
      throw err;
    }
    return etag;
  }

  private key(stateName: string, grainId: GrainId): string {
    return `${this.keyPrefix}:tx:${grainId.toString()}/${stateName}`;
  }
}
