import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import type { GrainStorage, StateHolder } from "@thresh/core/grain-storage";

interface Record {
  /** The state as the durable stores hold it: the value codec's JSON text. */
  data: string;
  etag: string;
}

/**
 * In-memory storage provider for development and tests. Not durable and not
 * shared across silos; mirrors Orleans `MemoryGrainStorage`. State round-trips through the value
 * codec exactly as the Redis and Postgres stores do, so a test against this store sees the same
 * values (a `GrainId` stays a `GrainId`) and the same failures (`UnsupportedValueError`) as production. Etag is a per-store
 * counter, giving the same optimistic-concurrency behaviour as a real provider.
 */
export class MemoryGrainStorage implements GrainStorage {
  private readonly records = new Map<string, Record>();
  private etagCounter = 0;

  async read<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void> {
    const record = this.records.get(this.key(stateName, grainId));
    if (record === undefined) {
      state.exists = false;
      state.etag = undefined;
      return;
    }
    state.value = deserializeValue<T>(record.data);
    state.etag = record.etag;
    state.exists = true;
  }

  async write<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void> {
    const key = this.key(stateName, grainId);
    const stored = this.records.get(key);
    if (this.isConflict(stored, state.etag)) {
      throw new InconsistentStateError(
        `etag conflict writing ${stateName} for ${grainId.toString()}`,
        state.etag,
        stored?.etag,
      );
    }
    // Encode before taking an etag, so a value the codec refuses leaves the store untouched.
    const data = serializeValue(state.value);
    const etag = String(++this.etagCounter);
    this.records.set(key, { data, etag });
    state.etag = etag;
    state.exists = true;
  }

  async clear<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void> {
    const key = this.key(stateName, grainId);
    const stored = this.records.get(key);
    if (this.isConflict(stored, state.etag)) {
      throw new InconsistentStateError(
        `etag conflict clearing ${stateName} for ${grainId.toString()}`,
        state.etag,
        stored?.etag,
      );
    }
    this.records.delete(key);
    state.etag = undefined;
    state.exists = false;
  }

  /**
   * Mirrors Orleans' `MemoryStorageGrain.ValidateEtag`: a caller's etag must
   * match the stored one when a record exists. When it does not, a blank
   * caller etag is a legitimate first write, but a non-empty one is a
   * conflict (issue #109) — the record can only have gone missing by being
   * cleared or resurrected under someone else's nose, and the caller's stale
   * etag says it does not know that.
   */
  private isConflict(stored: Record | undefined, expected: string | undefined): boolean {
    return stored !== undefined ? stored.etag !== expected : Boolean(expected);
  }

  private key(stateName: string, grainId: GrainId): string {
    return `${grainId.toString()}/${stateName}`;
  }
}
