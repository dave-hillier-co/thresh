import type { GrainId } from "@thresh/core/grain-id";
import { getPersistentFields } from "@thresh/core/persistent-state-metadata";
import { PersistentStateImpl } from "@thresh/persistence/persistent-state-impl";
import type { StorageRegistry } from "@thresh/persistence/storage-registry";

const emptyDefault = () => ({});

/**
 * Inject a `PersistentState` facet into each `@persistentState` field of a grain
 * instance and read it, before `onActivate`. Wired into the catalog by the
 * hosting layer so the runtime stays free of a persistence dependency.
 *
 * Pass `{ read: false }` on the migration-rehydration path: the facet is bound but
 * not read, so the migrated value restored from the bag (via `onRehydrate`) is not
 * overwritten by stale storage.
 */
export async function bindPersistentStates(
  instance: object,
  grainId: GrainId,
  registry: StorageRegistry,
  opts: { read?: boolean } = {},
): Promise<void> {
  const read = opts.read ?? true;
  for (const field of getPersistentFields(instance)) {
    const storage = registry.get(field.provider);
    const state = new PersistentStateImpl(
      field.stateName,
      grainId,
      storage,
      field.defaultValue ?? emptyDefault,
    );
    (instance as Record<string, unknown>)[field.fieldName] = state;
    if (read) await state.read();
  }
}
