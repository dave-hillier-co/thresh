import type { GrainId } from "./grain-id";

/** The mutable cell a storage provider reads into and writes from. */
export interface StateHolder<T> {
  value: T;
  etag?: string | undefined;
  exists: boolean;
}

/**
 * A pluggable storage provider, mirroring Orleans `IGrainStorage`. The provider
 * owns serialization, the storage mechanics and etag handling; the runtime owns
 * when to call it. Writes are conditional on the etag the grain last read.
 */
export interface GrainStorage {
  read<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
  write<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
  clear<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
}
