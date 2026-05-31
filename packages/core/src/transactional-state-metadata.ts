import { createFieldRegistry } from "./field-registry";

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
const registry = createFieldRegistry<TransactionalStateField>();

export const registerTransactionalField = registry.register;
export const getTransactionalFields = registry.getFields;
