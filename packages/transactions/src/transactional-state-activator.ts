import type { GrainId } from "@tsva/core/grain-id";
import { getTransactionalFields } from "@tsva/core/transactional-state-metadata";
import { TransactionalStateImpl } from "@tsva/transactions/transactional-state-impl";
import type { TransactionalStorageRegistry } from "@tsva/transactions/transactional-storage-registry";

/**
 * Inject a `TransactionalState` facet into each `@transactionalState` /
 * `useTransactionalState` field of a grain instance and load its committed
 * version, before `onActivate`. Wired into the catalog by the hosting layer
 * alongside the persistent/reducer binders.
 */
export async function bindTransactionalStates(
  instance: object,
  grainId: GrainId,
  registry: TransactionalStorageRegistry,
): Promise<void> {
  for (const field of getTransactionalFields(instance)) {
    const storage = registry.get();
    const state = new TransactionalStateImpl(field.stateName, grainId, field.initial, storage);
    (instance as Record<string, unknown>)[field.fieldName] = state;
    await state.load();
  }
}
