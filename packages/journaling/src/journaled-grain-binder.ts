import type { GrainId } from "@thresh/core/grain-id";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import type { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";
import { LogViewAdaptorImpl } from "@thresh/journaling/log-view-adaptor-impl";

/**
 * Installs a `LogViewAdaptor` on a `JournaledGrain` instance and replays its
 * confirmed log from durable storage, before `onActivate` — the
 * log-consistency analogue of `bindDurableStates`/`bindPersistentStates`. A
 * no-op for grains that are not `JournaledGrain`s. Wired into the catalog by
 * the hosting layer alongside the other state binders.
 */
export async function bindJournaledGrain(
  instance: object,
  grainId: GrainId,
  registry: JournalStorageRegistry,
  opts: { replay?: boolean; snapshotThreshold?: number; provider?: string } = {},
): Promise<void> {
  if (!(instance instanceof JournaledGrain)) return;

  const storage = registry.get(opts.provider);
  const manager = new StateMachineManagerImpl("journal", grainId, storage, {
    ...(opts.snapshotThreshold !== undefined ? { snapshotThreshold: opts.snapshotThreshold } : {}),
  });

  const adaptor = new LogViewAdaptorImpl(
    () => instance.initialState(),
    (state, event) => instance.transitionState(state, event),
    manager,
  );

  manager.register(adaptor);
  instance.installLogViewAdaptor(adaptor);

  if (opts.replay ?? true) await manager.replay();
}
