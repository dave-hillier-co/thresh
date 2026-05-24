/** A persistent-state field declared on a grain via `@persistentState`. */
export interface PersistentStateField {
  fieldName: string;
  stateName: string;
  provider?: string;
  defaultValue?: () => unknown;
}

// Per-instance registry populated by the decorator's initializer during
// construction; the runtime reads it to inject and read the facets before
// `onActivate`. Keyed by instance (a WeakMap) to avoid leaning on Symbol.metadata.
const registry = new WeakMap<object, PersistentStateField[]>();

export function registerPersistentField(instance: object, field: PersistentStateField): void {
  const list = registry.get(instance);
  if (list === undefined) registry.set(instance, [field]);
  else list.push(field);
}

export function getPersistentFields(instance: object): readonly PersistentStateField[] {
  return registry.get(instance) ?? [];
}
