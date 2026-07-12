import type { GrainId } from "@tsva/core/grain-id";
import type {
  DehydrationContext,
  IGrainMigrationParticipant,
  RehydrationContext,
} from "@tsva/core/grain-migration-participant";
import type { GrainStorage, StateHolder } from "@tsva/core/grain-storage";
import type { PersistentState } from "@tsva/core/persistent-state";
import { withStorageReadSpan } from "@tsva/observability/activation-tracing";

/**
 * Binds a named state to a grain identity and a storage provider. Keeps the
 * state in memory between writes (reads are served without touching the store)
 * and carries the etag for optimistic concurrency.
 *
 * Participates in grain migration: its in-memory value and etag travel in the
 * migration bag so a moved activation keeps even unflushed state and skips the
 * storage read on the target.
 */
export class PersistentStateImpl<T> implements PersistentState<T>, IGrainMigrationParticipant {
  private readonly holder: StateHolder<T>;

  constructor(
    private readonly stateName: string,
    private readonly grainId: GrainId,
    private readonly storage: GrainStorage,
    private readonly defaultValue: () => T,
  ) {
    this.holder = { value: defaultValue(), exists: false };
  }

  get value(): T {
    return this.holder.value;
  }

  set value(next: T) {
    this.holder.value = next;
  }

  get etag(): string | undefined {
    return this.holder.etag;
  }

  get exists(): boolean {
    return this.holder.exists;
  }

  async read(): Promise<void> {
    await withStorageReadSpan(
      {
        provider: this.storage.constructor.name,
        stateName: this.stateName,
        grainId: this.grainId.toString(),
      },
      () => this.storage.read(this.stateName, this.grainId, this.holder),
    );
  }

  async write(): Promise<void> {
    await this.storage.write(this.stateName, this.grainId, this.holder);
  }

  async clear(): Promise<void> {
    await this.storage.clear(this.stateName, this.grainId, this.holder);
    this.holder.value = this.defaultValue();
  }

  private get migrationKey(): string {
    return `persistentState:${this.stateName}`;
  }

  onDehydrate(context: DehydrationContext): void {
    context.set(this.migrationKey, {
      value: this.holder.value,
      etag: this.holder.etag,
      exists: this.holder.exists,
    });
  }

  onRehydrate(context: RehydrationContext): void {
    const snapshot = context.get<{ value: T; etag?: string; exists: boolean }>(this.migrationKey);
    if (snapshot === undefined) return;
    this.holder.value = snapshot.value;
    this.holder.exists = snapshot.exists;
    this.holder.etag = snapshot.etag;
  }
}
