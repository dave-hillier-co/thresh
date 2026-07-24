import type { GrainId } from "@tsva/core/grain-id";
import { getTransactionalFields } from "@tsva/core/transactional-state-metadata";
import type { TimeProvider } from "@tsva/core/time-provider";
import {
  TransactionalStateImpl,
  type ResolveStatus,
} from "@tsva/transactions/transactional-state-impl";
import type { TransactionalStorageRegistry } from "@tsva/transactions/transactional-storage-registry";

/** Every `TransactionalState` facet bound onto an instance, so `unbindTransactionalStates` can unload them. */
const boundStates = new WeakMap<object, TransactionalStateImpl<unknown>[]>();

/**
 * Inject a `TransactionalState` facet into each `@transactionalState` /
 * `useTransactionalState` field of a grain instance and load its committed
 * version, before `onActivate`. `resolveStatus` (when provided by the host) lets
 * a facet resolve an in-doubt pending record against its TM on activation; if
 * the TM is unreachable, the facet's confirmation worker keeps retrying until
 * {@link unbindTransactionalStates} stops it (call on deactivation). `time`
 * (when provided by the host) is the confirmation worker's clock, so tests
 * can drive its backoff deterministically instead of the system clock.
 */
export async function bindTransactionalStates(
  instance: object,
  grainId: GrainId,
  registry: TransactionalStorageRegistry,
  resolveStatus?: ResolveStatus,
  time?: TimeProvider,
): Promise<void> {
  const states = boundStates.get(instance) ?? [];
  for (const field of getTransactionalFields(instance)) {
    const storage = registry.get();
    const state = new TransactionalStateImpl(
      field.stateName,
      grainId,
      field.initial,
      storage,
      resolveStatus,
      undefined,
      time,
    );
    (instance as Record<string, unknown>)[field.fieldName] = state;
    await state.load();
    states.push(state as TransactionalStateImpl<unknown>);
  }
  boundStates.set(instance, states);
}

/** Stop every bound facet's confirmation worker for this instance (call on deactivation). */
export function unbindTransactionalStates(instance: object): void {
  for (const state of boundStates.get(instance) ?? []) state.unload();
  boundStates.delete(instance);
}
