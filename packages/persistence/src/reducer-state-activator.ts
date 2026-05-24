import type { GrainId } from "@tsva/core/grain-id";
import { getReducerFields } from "@tsva/core/reducer-state-metadata";
import { ReducerStateImpl } from "@tsva/persistence/reducer-state-impl";
import type { StorageRegistry } from "@tsva/persistence/storage-registry";

/**
 * Inject a `ReducerState` facet into each `@reducerState` field of a grain
 * instance and read its snapshot, before `onActivate`. Wired into the catalog by
 * the hosting layer alongside `bindPersistentStates`.
 */
export async function bindReducerStates(
  instance: object,
  grainId: GrainId,
  registry: StorageRegistry,
): Promise<void> {
  for (const field of getReducerFields(instance)) {
    const storage = registry.get(field.provider);
    const state = new ReducerStateImpl(
      field.stateName,
      grainId,
      storage,
      field.initial,
      field.reduce,
    );
    (instance as Record<string, unknown>)[field.fieldName] = state;
    await state.read();
  }
}
