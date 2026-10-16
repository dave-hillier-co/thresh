import type { JournalStorage } from "@thresh/core/journal-storage";

export const DEFAULT_JOURNAL_PROVIDER = "default";

/** Named journal-storage providers configured on a silo; resolves a durable field's provider. */
export class JournalStorageRegistry {
  private readonly providers = new Map<string, JournalStorage>();
  private fallback: JournalStorage | undefined;

  add(name: string, storage: JournalStorage): this {
    this.providers.set(name, storage);
    return this;
  }

  /**
   * The provider that serves every name nothing is registered under — a silo's
   * storage backend (`SiloBuilder.useStorageBackend`).
   */
  setFallback(storage: JournalStorage): this {
    this.fallback = storage;
    return this;
  }

  /** The providers registered by name; the fallback is not among them. */
  named(): ReadonlyMap<string, JournalStorage> {
    return this.providers;
  }

  has(name: string): boolean {
    return this.providers.has(name) || this.fallback !== undefined;
  }

  get(name: string = DEFAULT_JOURNAL_PROVIDER): JournalStorage {
    const storage = this.providers.get(name) ?? this.fallback;
    if (storage === undefined) throw new Error(`no journal storage provider registered: ${name}`);
    return storage;
  }
}
