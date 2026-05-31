import { createFieldRegistry } from "./field-registry";

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
const registry = createFieldRegistry<PersistentStateField>();

export const registerPersistentField = registry.register;
export const getPersistentFields = registry.getFields;
