import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage, StateHolder } from "@thresh/core/grain-storage";

interface Record {
  value: unknown;
  etag: string;
}

/**
 * In-memory storage provider for development and tests. Not durable and not
 * shared across silos; mirrors Orleans `MemoryGrainStorage`. Etag is a per-store
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
    state.value = structuredClone(record.value) as T;
    state.etag = record.etag;
    state.exists = true;
  }

  async write<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void> {
    const key = this.key(stateName, grainId);
    const stored = this.records.get(key);
    if (stored !== undefined && stored.etag !== state.etag) {
      throw new InconsistentStateError(
        `etag conflict writing ${stateName} for ${grainId.toString()}`,
        state.etag,
        stored.etag,
      );
    }
    const etag = String(++this.etagCounter);
    this.records.set(key, { value: structuredClone(state.value), etag });
    state.etag = etag;
    state.exists = true;
  }

  async clear<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void> {
    const key = this.key(stateName, grainId);
    const stored = this.records.get(key);
    if (stored !== undefined && stored.etag !== state.etag) {
      throw new InconsistentStateError(
        `etag conflict clearing ${stateName} for ${grainId.toString()}`,
        state.etag,
        stored.etag,
      );
    }
    this.records.delete(key);
    state.etag = undefined;
    state.exists = false;
  }

  private key(stateName: string, grainId: GrainId): string {
    return `${grainId.toString()}/${stateName}`;
  }
}
