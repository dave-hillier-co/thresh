import type { GrainId } from "@thresh/core/grain-id";
import { getReducerFields } from "@thresh/core/reducer-state-metadata";
import { ReducerStateImpl } from "@thresh/persistence/reducer-state-impl";
import type { StorageRegistry } from "@thresh/persistence/storage-registry";

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
