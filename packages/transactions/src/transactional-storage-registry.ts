import type { TransactionalStateStorage } from "@tsva/core/transactional-storage";

export const DEFAULT_TRANSACTIONAL_PROVIDER = "default";

/** Named transactional-storage providers configured on a silo. */
export class TransactionalStorageRegistry {
  private readonly providers = new Map<string, TransactionalStateStorage>();

  add(name: string, storage: TransactionalStateStorage): this {
    this.providers.set(name, storage);
    return this;
  }

  has(name: string): boolean {
    return this.providers.has(name);
  }

  get(name: string = DEFAULT_TRANSACTIONAL_PROVIDER): TransactionalStateStorage {
    const storage = this.providers.get(name);
    if (storage === undefined)
      throw new Error(`no transactional storage provider registered: ${name}`);
    return storage;
  }
}
