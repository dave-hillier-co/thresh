import type { GrainId } from "@tsva/core/grain-id";
import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import { getDurableFields, type DurableKind } from "@tsva/core/durable-state-metadata";
import { DurableValueImpl } from "@tsva/journaling/durable-value-impl";
import { DurableDictionaryImpl } from "@tsva/journaling/durable-dictionary-impl";
import { DurableListImpl } from "@tsva/journaling/durable-list-impl";
import type { JournalStorageRegistry } from "@tsva/journaling/journal-storage-registry";
import { StateMachineManagerImpl } from "@tsva/journaling/state-machine-manager-impl";

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
  }
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
  const providerName = fields[0]!.provider;
  for (const field of fields) {
    if (field.provider !== providerName) {
      throw new Error(`durable fields on ${grainId.toString()} must share one journal provider`);
    }
  }

  const storage = registry.get(providerName);
  const manager = new StateMachineManagerImpl("journal", grainId, storage, {
    ...(opts.snapshotThreshold !== undefined ? { snapshotThreshold: opts.snapshotThreshold } : {}),
  });

  for (const field of fields) {
    const machine = makeMachine(field.kind, field.stateName, manager);
    manager.register(machine);
    (instance as Record<string, unknown>)[field.fieldName] = machine;
  }

  if (opts.replay ?? true) await manager.replay();
}
