import type { GrainId } from "@thresh/core/grain-id";
import { getDurableFields } from "@thresh/core/durable-state-metadata";
import { isCustomStorageHost, JournaledGrain } from "@thresh/core/journaled-grain";
import {
  bindDurableStates,
  durableFieldsProviderName,
  registerDurableMachines,
} from "@thresh/journaling/durable-state-activator";
import {
  bindJournaledGrain,
  installJournalViewAdaptor,
} from "@thresh/journaling/journaled-grain-binder";
import type { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

/**
 * Binds every journal-substrate facet on a grain instance -- its
 * `@durableState`/`@durableDictionary`/`@durableList`/... fields AND, if it is
 * a `JournaledGrain`, its log-view adaptor -- onto ONE shared
 * `StateMachineManager`, then replays once.
 *
 * Both facets append to the same per-grain log (mirroring
 * `silo-builder.ts`'s "one manager per grain owns the log"), so building two
 * separate managers over it -- as `bindDurableStates` and `bindJournaledGrain`
 * did when called independently -- makes each manager see the other's entries
 * as an unregistered machine: they get retired and purged after two
 * compactions, and the two version counters invalidate each other on every
 * append. This is the entry point the hosting layer uses instead of calling
 * `bindDurableStates` and `bindJournaledGrain` separately; those two remain
 * available (and correct) for a grain that only uses one of the two facets.
 *
 * A `JournaledGrain` that also implements `CustomStorageInterface` owns its
 * own log persistence and never touches the journal substrate at all, and a
 * `JournaledGrain` whose durable fields name a different journal store writes
 * its log elsewhere; neither shares a log with its durable fields, so both are
 * bound through `bindDurableStates` + `bindJournaledGrain` unchanged.
 */
export async function bindJournalFacets(
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
  const isJournaledGrain = instance instanceof JournaledGrain;
  const fields = getDurableFields(instance);
  const durableProvider = durableFieldsProviderName(fields, grainId);

  // Only a substrate-backed JournaledGrain whose log lands on the SAME journal
  // store as the durable fields actually shares a log with them. A
  // custom-storage host persists its events itself, and a JournaledGrain on a
  // different provider from its fields writes to a different store, so in
  // both cases the two facets never collide -- bind each on its own (keeping
  // existing history where it has always lived).
  const sharesLog =
    isJournaledGrain &&
    !isCustomStorageHost(instance) &&
    fields.length > 0 &&
    registry.get(durableProvider) === registry.get(opts.provider);
  if (!sharesLog) {
    await bindDurableStates(instance, grainId, registry, opts);
    await bindJournaledGrain(instance, grainId, registry, opts);
    return;
  }

  const manager = new StateMachineManagerImpl("journal", grainId, registry.get(opts.provider), {
    ...(opts.snapshotThreshold !== undefined ? { snapshotThreshold: opts.snapshotThreshold } : {}),
  });

  registerDurableMachines(instance, fields, manager);
  installJournalViewAdaptor(instance, manager);

  if (opts.replay ?? true) await manager.replay();
}
