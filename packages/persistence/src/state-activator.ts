import type { GrainId } from "@tsva/core/grain-id";
import { getPersistentFields } from "@tsva/core/persistent-state-metadata";
import { PersistentStateImpl } from "@tsva/persistence/persistent-state-impl";
import type { StorageRegistry } from "@tsva/persistence/storage-registry";

const emptyDefault = () => ({});

/**
 * Inject a `PersistentState` facet into each `@persistentState` field of a grain
 * instance and read it, before `onActivate`. Wired into the catalog by the
 * hosting layer so the runtime stays free of a persistence dependency.
 */
export async function bindPersistentStates(
  instance: object,
  grainId: GrainId,
  registry: StorageRegistry,
): Promise<void> {
  for (const field of getPersistentFields(instance)) {
    const storage = registry.get(field.provider);
    const state = new PersistentStateImpl(
      field.stateName,
      grainId,
      storage,
      field.defaultValue ?? emptyDefault,
    );
    (instance as Record<string, unknown>)[field.fieldName] = state;
    await state.read();
  }
}
