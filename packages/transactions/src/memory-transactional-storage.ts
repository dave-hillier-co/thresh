import { InconsistentStateError } from "@tsva/core/errors";
import type { GrainId } from "@tsva/core/grain-id";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@tsva/core/transactional-storage";
import { decodeValue, encodeValue } from "@tsva/core/value-codec";
import {
  applyStore,
  EMPTY_METADATA,
  emptyRecord,
  type StoredRecord,
} from "@tsva/transactions/transactional-storage-apply";

// Clone through the value-codec so a record's runtime types (GrainId, Date)
// survive load/store — matching the Redis provider's serialization fidelity.
const clone = <T>(value: T): T => decodeValue(encodeValue(value)) as T;

interface Entry {
  etag: string;
  record: StoredRecord<unknown>;
}

/**
 * In-memory `TransactionalStateStorage` for fast sociable tests and as the
 * default when no durable provider is configured. A single instance shared
 * across "restarts" stands in for a durable backend. The store/commit/abort
 * semantics ({@link applyStore}) match the Redis provider so behaviour is
 * identical.
 */
export class MemoryTransactionalStorage implements TransactionalStateStorage {
  private readonly entries = new Map<string, Entry>();
  private etagCounter = 0;

  private key(stateName: string, grainId: GrainId): string {
    return `${grainId.toString()}/${stateName}`;
  }

  async load(
    stateName: string,
    grainId: GrainId,
  ): Promise<TransactionalStorageLoadResponse<unknown>> {
    const entry = this.entries.get(this.key(stateName, grainId));
    if (entry === undefined) {
      return {
        etag: undefined,
        committedState: undefined,
        committedSequenceId: 0,
        metadata: EMPTY_METADATA,
        pendingStates: [],
      };
    }
    const record = clone(entry.record);
    return {
      etag: entry.etag,
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
    const entry = this.entries.get(key);
    if ((entry?.etag ?? undefined) !== expectedETag) {
      throw new InconsistentStateError(
        `transactional store etag conflict for ${key}`,
        expectedETag,
        entry?.etag,
      );
    }
    const record = entry?.record ?? emptyRecord<unknown>();
    applyStore(record, metadata, statesToPrepare, commitUpTo, abortAfter);
    const etag = String(++this.etagCounter);
    this.entries.set(key, { etag, record });
    return etag;
  }
}
