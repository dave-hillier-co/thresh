import type { GrainId } from "@thresh/core/grain-id";
import { getTransactionalFields } from "@thresh/core/transactional-state-metadata";
import type { TimeProvider } from "@thresh/core/time-provider";
import type { TransactionProtocol } from "@thresh/core/transaction-resource";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";
import type { TransactionalStorageRegistry } from "@thresh/transactions/transactional-storage-registry";

/** Every `TransactionalState` facet bound onto an instance, so `unbindTransactionalStates` can unload them. */
const boundStates = new WeakMap<object, TransactionalStateImpl<unknown>[]>();

/**
 * Inject a `TransactionalState` facet into each `@transactionalState` /
 * `useTransactionalState` field of a grain instance and load it, before
 * `onActivate`. `protocol` (when provided by the host) is how a facet sends
 * commit-protocol messages to other resources: its votes and pings to a TM,
 * and as TM its confirmations and cancellations. A facet that loads prepared
 * records resumes them through it. `time` (when provided by the host) is the
 * clock of the facet's protocol timers, so tests can drive them
 * deterministically instead of the system clock. Call
 * {@link unbindTransactionalStates} on deactivation.
 */
export async function bindTransactionalStates(
  instance: object,
  grainId: GrainId,
  registry: TransactionalStorageRegistry,
  protocol?: TransactionProtocol,
  time?: TimeProvider,
  options?: Partial<TransactionsOptions>,
): Promise<void> {
  const states = boundStates.get(instance) ?? [];
  for (const field of getTransactionalFields(instance)) {
    const storage = registry.get(field.provider);
    const state = new TransactionalStateImpl(
      field.stateName,
      grainId,
      field.initial,
      storage,
      protocol,
      options,
      time,
    );
    (instance as Record<string, unknown>)[field.fieldName] = state;
    await state.load();
    states.push(state as TransactionalStateImpl<unknown>);
  }
  boundStates.set(instance, states);
}

/** Stop every bound facet's workers and timers for this instance (call on deactivation). */
export function unbindTransactionalStates(instance: object): void {
  for (const state of boundStates.get(instance) ?? []) state.unload();
  boundStates.delete(instance);
}
