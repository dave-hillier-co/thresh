import type { GrainStorage } from "@thresh/core/grain-storage";

export const DEFAULT_PROVIDER = "default";

/** Named storage providers configured on a silo; resolves a `@persistentState`'s provider. */
export class StorageRegistry {
  private readonly providers = new Map<string, GrainStorage>();
  private fallback: GrainStorage | undefined;

  add(name: string, storage: GrainStorage): this {
    this.providers.set(name, storage);
    return this;
  }

  /**
   * The provider that serves every name nothing is registered under — a silo's
   * storage backend (`SiloBuilder.useStorageBackend`).
   */
  setFallback(storage: GrainStorage): this {
    this.fallback = storage;
    return this;
  }

  /** The providers registered by name; the fallback is not among them. */
  named(): ReadonlyMap<string, GrainStorage> {
    return this.providers;
  }

  /** The provider registered under `name` (else the fallback), or `undefined` when neither is. */
  tryGet(name: string = DEFAULT_PROVIDER): GrainStorage | undefined {
    return this.providers.get(name) ?? this.fallback;
  }

  get(name: string = DEFAULT_PROVIDER): GrainStorage {
    const storage = this.tryGet(name);
    if (storage === undefined) throw new Error(`no storage provider registered: ${name}`);
    return storage;
  }
}
