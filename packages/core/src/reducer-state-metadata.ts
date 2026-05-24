/** A reducer-state field declared on a grain via `@reducerState`. */
export interface ReducerStateField {
  fieldName: string;
  stateName: string;
  provider?: string;
  initial: () => unknown;
  reduce: (state: unknown, event: unknown) => unknown;
}

// Per-instance registry populated by the decorator's initializer during
// construction; the runtime reads it to inject and read the facets before
// `onActivate`. Mirrors persistent-state-metadata.
const registry = new WeakMap<object, ReducerStateField[]>();

export function registerReducerField(instance: object, field: ReducerStateField): void {
  const list = registry.get(instance);
  if (list === undefined) registry.set(instance, [field]);
  else list.push(field);
}

export function getReducerFields(instance: object): readonly ReducerStateField[] {
  return registry.get(instance) ?? [];
}
