import type { GrainStorage } from "@thresh/core/grain-storage";

export const DEFAULT_PROVIDER = "default";

/** Named storage providers configured on a silo; resolves a `@persistentState`'s provider. */
export class StorageRegistry {
  private readonly providers = new Map<string, GrainStorage>();

  add(name: string, storage: GrainStorage): this {
    this.providers.set(name, storage);
    return this;
  }

  /** The provider registered under `name`, or `undefined` when nothing is. */
  tryGet(name: string = DEFAULT_PROVIDER): GrainStorage | undefined {
    return this.providers.get(name);
  }

  get(name: string = DEFAULT_PROVIDER): GrainStorage {
    const storage = this.providers.get(name);
    if (storage === undefined) throw new Error(`no storage provider registered: ${name}`);
    return storage;
  }
}
