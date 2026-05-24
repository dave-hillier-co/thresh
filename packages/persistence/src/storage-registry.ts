import type { GrainStorage } from "@tsva/core/grain-storage";

export const DEFAULT_PROVIDER = "default";

/** Named storage providers configured on a silo; resolves a `@persistentState`'s provider. */
export class StorageRegistry {
  private readonly providers = new Map<string, GrainStorage>();

  add(name: string, storage: GrainStorage): this {
    this.providers.set(name, storage);
    return this;
  }

  has(name: string): boolean {
    return this.providers.has(name);
  }

  get(name: string = DEFAULT_PROVIDER): GrainStorage {
    const storage = this.providers.get(name);
    if (storage === undefined) throw new Error(`no storage provider registered: ${name}`);
    return storage;
  }
}
