import type { JournalStorage } from "@tsva/core/journal-storage";

export const DEFAULT_JOURNAL_PROVIDER = "default";

/** Named journal-storage providers configured on a silo; resolves a durable field's provider. */
export class JournalStorageRegistry {
  private readonly providers = new Map<string, JournalStorage>();

  add(name: string, storage: JournalStorage): this {
    this.providers.set(name, storage);
    return this;
  }

  has(name: string): boolean {
    return this.providers.has(name);
  }

  get(name: string = DEFAULT_JOURNAL_PROVIDER): JournalStorage {
    const storage = this.providers.get(name);
    if (storage === undefined) throw new Error(`no journal storage provider registered: ${name}`);
    return storage;
  }
}
