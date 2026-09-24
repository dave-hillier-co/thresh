import type { GrainId } from "@thresh/core/grain-id";
import type { StateMachineManager } from "@thresh/core/durable-state-machine";
import { isCustomStorageHost, JournaledGrain } from "@thresh/core/journaled-grain";
import type { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";
import { LogViewAdaptorImpl } from "@thresh/journaling/log-view-adaptor-impl";
import { CustomStorageLogViewAdaptorImpl } from "@thresh/journaling/custom-storage-log-view-adaptor-impl";

/**
 * Registers a `LogViewAdaptorImpl` for `instance` on `manager` and installs it,
 * without replaying. Shared by `bindJournaledGrain` (which owns the manager
 * outright) and `bindJournalFacets` (which shares one manager with the
 * grain's `@durableState` fields -- see its module doc). Only for the
 * journal-substrate case: a `CustomStorageInterface` host bypasses the
 * journal substrate entirely and is not affected by manager sharing.
 */
export function installJournalViewAdaptor<TState, TEvent>(
  instance: JournaledGrain<TState, TEvent>,
  manager: StateMachineManager,
): void {
  const adaptor = new LogViewAdaptorImpl<TState, TEvent>(
    () => instance.initialState(),
    (state, event) => instance.transitionState(state, event),
    manager,
  );
  manager.register(adaptor);
  instance.installLogViewAdaptor(adaptor);
}

/**
 * Installs a `LogViewAdaptor` on a `JournaledGrain` instance and replays its
 * confirmed log from durable storage, before `onActivate` — the
 * log-consistency analogue of `bindDurableStates`/`bindPersistentStates`. A
 * no-op for grains that are not `JournaledGrain`s. Wired into the catalog by
 * the hosting layer alongside the other state binders.
 *
 * A grain that also implements `CustomStorageInterface` owns its own log persistence, so it is
 * bound to the custom-storage adaptor and the journal substrate is not touched at all -- the
 * same fork Orleans makes by configuring the CustomStorage log-consistency provider instead of
 * the state/log-storage ones.
 */
export async function bindJournaledGrain(
  instance: object,
  grainId: GrainId,
  registry: JournalStorageRegistry,
  opts: {
    replay?: boolean;
    snapshotThreshold?: number;
    provider?: string;
    /** CAS attempts per confirm, for a custom-storage host. Ignored otherwise. */
    maxAttempts?: number;
  } = {},
): Promise<void> {
  if (!(instance instanceof JournaledGrain)) return;

  if (isCustomStorageHost(instance)) {
    const adaptor = new CustomStorageLogViewAdaptorImpl(
      () => instance.initialState(),
      (state, event) => instance.transitionState(state, event),
      instance,
      opts.maxAttempts !== undefined ? { maxAttempts: opts.maxAttempts } : {},
    );
    instance.installLogViewAdaptor(adaptor);
    if (opts.replay ?? true) await adaptor.read();
    return;
  }

  const storage = registry.get(opts.provider);
  const manager = new StateMachineManagerImpl("journal", grainId, storage, {
    ...(opts.snapshotThreshold !== undefined ? { snapshotThreshold: opts.snapshotThreshold } : {}),
  });

  installJournalViewAdaptor(instance, manager);

  if (opts.replay ?? true) await manager.replay();
}
