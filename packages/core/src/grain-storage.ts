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
  /**
   * `signal`, when given, is an ambient cancellation/deadline signal for this
   * call (Orleans has no analogue — see `docs/deviations.md`). A provider
   * honours it on a best-effort basis against its own network calls (e.g.
   * `RedisGrainStorage` via node-redis's `withAbortSignal`); a provider that
   * cannot cancel mid-flight (e.g. `PostgresGrainStorage`, whose client has no
   * abort hook) may instead only abandon the *wait* for an already-sent call
   * (`@thresh/core/abort`'s `raceSignal`), or ignore it entirely.
   */
  read<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void>;
  write<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void>;
  clear<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void>;
}
