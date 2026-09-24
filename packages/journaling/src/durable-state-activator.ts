import type { GrainId } from "@thresh/core/grain-id";
import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import {
  getDurableFields,
  type DurableKind,
  type DurableStateField,
} from "@thresh/core/durable-state-metadata";
import { DurableValueImpl } from "@thresh/journaling/durable-value-impl";
import { DurableDictionaryImpl } from "@thresh/journaling/durable-dictionary-impl";
import { DurableListImpl } from "@thresh/journaling/durable-list-impl";
import { DurableQueueImpl } from "@thresh/journaling/durable-queue-impl";
import { DurableSetImpl } from "@thresh/journaling/durable-set-impl";
import type { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

function makeMachine(
  kind: DurableKind,
  stateName: string,
  manager: StateMachineManager,
): DurableStateMachine {
  switch (kind) {
    case "value":
      return new DurableValueImpl(stateName, manager);
    case "dictionary":
      return new DurableDictionaryImpl(stateName, manager);
    case "list":
      return new DurableListImpl(stateName, manager);
    case "queue":
      return new DurableQueueImpl(stateName, manager);
    case "set":
      return new DurableSetImpl(stateName, manager);
  }
}

/**
 * Builds and registers a `DurableStateMachine` on `manager` for each of the
 * instance's `@durableState`-family fields, assigning it back onto the
 * instance. Shared by `bindDurableStates` (which owns the manager outright)
 * and `bindJournalFacets` (which shares one manager with a `JournaledGrain`
 * adaptor on the same grain -- see its module doc).
 */
export function registerDurableMachines(
  instance: object,
  fields: readonly DurableStateField[],
  manager: StateMachineManager,
): void {
  for (const field of fields) {
    const machine = makeMachine(field.kind, field.stateName, manager);
    manager.register(machine);
    (instance as Record<string, unknown>)[field.fieldName] = machine;
  }
}

/**
 * Resolves the single journal-storage provider name declared across
 * `instance`'s durable fields, throwing if they disagree. `undefined` when
 * there are no durable fields (the caller then has only its own default to
 * fall back to).
 */
export function durableFieldsProviderName(
  fields: readonly DurableStateField[],
  grainId: GrainId,
): string | undefined {
  if (fields.length === 0) return undefined;
  const providerName = fields[0]!.provider;
  for (const field of fields) {
    if (field.provider !== providerName) {
      throw new Error(`durable fields on ${grainId.toString()} must share one journal provider`);
    }
  }
  return providerName;
}

/**
 * Inject the durable-journaling facets into a grain instance and replay its log,
 * before `onActivate`. All the grain's `@durableState` / `@durableDictionary` /
 * `@durableList` fields share ONE `StateMachineManager` (and one log), so this
 * builds the manager once, registers every structure on it, then replays once.
 * Wired into the catalog by the hosting layer alongside `bindPersistentStates`.
 */
export async function bindDurableStates(
  instance: object,
  grainId: GrainId,
  registry: JournalStorageRegistry,
  opts: { replay?: boolean; snapshotThreshold?: number } = {},
): Promise<void> {
  const fields = getDurableFields(instance);
  if (fields.length === 0) return;

  // One log per grain: all structures use the same provider; reject a mix.
  const providerName = durableFieldsProviderName(fields, grainId);

  const storage = registry.get(providerName);
  const manager = new StateMachineManagerImpl("journal", grainId, storage, {
    ...(opts.snapshotThreshold !== undefined ? { snapshotThreshold: opts.snapshotThreshold } : {}),
  });

  registerDurableMachines(instance, fields, manager);

  if (opts.replay ?? true) await manager.replay();
}
