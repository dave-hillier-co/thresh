import type { GrainId } from "@tsva/core/grain-id";
import { getTransactionalFields } from "@tsva/core/transactional-state-metadata";
import {
  TransactionalStateImpl,
  type ResolveStatus,
} from "@tsva/transactions/transactional-state-impl";
import type { TransactionalStorageRegistry } from "@tsva/transactions/transactional-storage-registry";

/**
 * Inject a `TransactionalState` facet into each `@transactionalState` /
 * `useTransactionalState` field of a grain instance and load its committed
 * version, before `onActivate`. `resolveStatus` (when provided by the host) lets
 * a facet resolve an in-doubt pending record against its TM on activation.
 */
export async function bindTransactionalStates(
  instance: object,
  grainId: GrainId,
  registry: TransactionalStorageRegistry,
  resolveStatus?: ResolveStatus,
): Promise<void> {
  for (const field of getTransactionalFields(instance)) {
    const storage = registry.get();
    const state = new TransactionalStateImpl(
      field.stateName,
      grainId,
      field.initial,
      storage,
      resolveStatus,
    );
    (instance as Record<string, unknown>)[field.fieldName] = state;
    await state.load();
  }
}
