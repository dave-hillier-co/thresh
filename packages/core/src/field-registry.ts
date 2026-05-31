/**
 * A per-instance field registry: the decorator initializers register fields on
 * the instance during construction and the runtime reads them before
 * `onActivate`. Keyed by instance (a WeakMap) to avoid leaning on
 * `Symbol.metadata`. Used identically by the persistent/reducer/transactional/
 * durable state-metadata modules.
 */
export function createFieldRegistry<T>(): {
  register(instance: object, field: T): void;
  getFields(instance: object): readonly T[];
} {
  const registry = new WeakMap<object, T[]>();
  return {
    register(instance: object, field: T): void {
      const list = registry.get(instance);
      if (list === undefined) registry.set(instance, [field]);
      else list.push(field);
    },
    getFields(instance: object): readonly T[] {
      return registry.get(instance) ?? [];
    },
  };
}
