import { randomUUID } from "node:crypto";
import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@thresh/core/transactional-storage";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import {
  applyStore,
  EMPTY_METADATA,
  emptyRecord,
  type StoredRecord,
} from "@thresh/transactions/transactional-storage-apply";

/** A connected node-redis client (the subset this provider drives). */
type RedisClient = {
  hGetAll(key: string): Promise<Record<string, string>>;
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  withAbortSignal(signal: AbortSignal): RedisClient;
};

export interface RedisTransactionalStorageOptions {
  /** Namespace for keys in the shared Redis (defaults to `"thresh"`). */
  keyPrefix?: string;
  /**
   * Logical service identity this provider's keys belong to (Orleans'
   * `ClusterOptions.ServiceId`). Two clusters pointed at the same Redis stay
   * partitioned by it. Defaults to `"default"`, Orleans' own
   * `ClusterOptions.DefaultServiceId`; `SiloBuilder` threads the silo's
   * `serviceId ?? DEFAULT_SERVICE_ID`.
   *
   * Redis has no ALTER: a record written before this option existed has no
   * `{serviceId}` segment and is invisible to a service-partitioned reader —
   * a deliberate upgrade break, the same call #59 made for `RedisGrainStorage`
   * (see todo.md / docs/deviations.md). A Redis-backed transactional
   * deployment should drain in-flight transactions before upgrading.
   */
  serviceId?: string;
}

const CONFLICT = "THRESH_TX_ETAG_CONFLICT";

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
  private readonly serviceId: string;

  constructor(
    private readonly client: RedisClient,
    options: RedisTransactionalStorageOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "thresh";
    this.serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
  }

  async load(
    stateName: string,
    grainId: GrainId,
    signal?: AbortSignal,
  ): Promise<TransactionalStorageLoadResponse<unknown>> {
    const hash = await this.clientFor(signal).hGetAll(this.key(stateName, grainId));
    if (hash.etag === undefined) {
      return {
        etag: undefined,
        committedState: undefined,
        committedSequenceId: 0,
        metadata: EMPTY_METADATA,
        pendingStates: [],
      };
    }
    const record = deserializeValue<StoredRecord<unknown>>(hash.data ?? "{}");
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
    signal?: AbortSignal,
  ): Promise<string> {
    const key = this.key(stateName, grainId);
    const client = this.clientFor(signal);
    const hash = await client.hGetAll(key);
    const record =
      hash.data !== undefined ? deserializeValue<StoredRecord<unknown>>(hash.data) : emptyRecord();
    applyStore(record, metadata, statesToPrepare, commitUpTo, abortAfter);

    const etag = randomUUID();
    try {
      await client.eval(WRITE, {
        keys: [key],
        arguments: [expectedETag ?? "", serializeValue(record), etag],
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

  /** The client to issue this call on: scoped to `signal` (node-redis's own abort hook) when given. */
  private clientFor(signal: AbortSignal | undefined): RedisClient {
    return signal === undefined ? this.client : this.client.withAbortSignal(signal);
  }

  private key(stateName: string, grainId: GrainId): string {
    return `${this.keyPrefix}:${this.serviceId}:tx:${grainId.toString()}/${stateName}`;
  }
}
