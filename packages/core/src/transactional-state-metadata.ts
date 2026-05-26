/** A transactional-state field declared on a grain via `@transactionalState`. */
export interface TransactionalStateField {
  fieldName: string;
  stateName: string;
  /** The state before any committed write. */
  initial: () => unknown;
}

// Per-instance registry populated by the decorator's initializer during
// construction; the runtime reads it to inject the facets before `onActivate`.
// Keyed by instance (a WeakMap), exactly like the persistent/reducer registries.
const registry = new WeakMap<object, TransactionalStateField[]>();

export function registerTransactionalField(instance: object, field: TransactionalStateField): void {
  const list = registry.get(instance);
  if (list === undefined) registry.set(instance, [field]);
  else list.push(field);
}

export function getTransactionalFields(instance: object): readonly TransactionalStateField[] {
  return registry.get(instance) ?? [];
}
