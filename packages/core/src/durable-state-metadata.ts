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

const registry = new WeakMap<object, DurableStateField[]>();

export function registerDurableField(instance: object, field: DurableStateField): void {
  const list = registry.get(instance);
  if (list === undefined) registry.set(instance, [field]);
  else list.push(field);
}

export function getDurableFields(instance: object): readonly DurableStateField[] {
  return registry.get(instance) ?? [];
}
