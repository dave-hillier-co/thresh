import { createFieldRegistry } from "./field-registry";

/** Which durable structure a journalled field declares. */
export type DurableKind = "value" | "dictionary" | "list" | "queue" | "set";

/**
 * A durable-journaling field declared on a grain via the `@durableState` /
 * `@durableDictionary` / `@durableList` decorators or the `useDurable*` hooks.
 * The binder builds the matching structure and registers it on the grain's
 * single state-machine manager.
 */
export interface DurableStateField {
  fieldName: string;
  /** Name under which this structure registers on the grain's manager. */
  stateName: string;
  kind: DurableKind;
  /** Journal storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

const registry = createFieldRegistry<DurableStateField>();

export const registerDurableField = registry.register;
export const getDurableFields = registry.getFields;

/**
 * What a grain's durable structures are written through: the per-grain state-machine manager,
 * seen from the grain side. `writeState` is Orleans' `WriteStateAsync`.
 */
export interface DurableStateWriter {
  writeState(): Promise<void>;
}

const writers = new WeakMap<object, DurableStateWriter>();

/** @internal Called by the journaling binders once a grain's manager exists. */
export function bindDurableStateWriter(instance: object, writer: DurableStateWriter): void {
  writers.set(instance, writer);
}

/**
 * Orleans' `DurableGrain.WriteStateAsync`: append everything buffered across every durable
 * structure of `instance` in one storage write. Rejects if the grain has no durable state bound.
 */
export function writeDurableState(instance: object): Promise<void> {
  const writer = writers.get(instance);
  if (writer === undefined) {
    return Promise.reject(
      new Error("this grain has no durable state bound (declare a @durable* field or useDurable*)"),
    );
  }
  return writer.writeState();
}
