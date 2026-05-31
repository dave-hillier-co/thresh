import { createFieldRegistry } from "./field-registry";

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
const registry = createFieldRegistry<ReducerStateField>();

export const registerReducerField = registry.register;
export const getReducerFields = registry.getFields;
