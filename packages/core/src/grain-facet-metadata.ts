import { createFieldRegistry } from "./field-registry";

/**
 * A third-party facet field declared on a grain via `@grainFacet` (or a
 * thin wrapper decorator built on it, e.g. a plugin's own `@exampleStorage`).
 * Mirrors `PersistentStateField`/`DurableStateField`, generalized: `kind`
 * names the facet family (matches the name a silo builder registers factories
 * under via `addGrainFacetFactory`), `providerName` selects which registered
 * factory binds this field (defaulting to whichever factory was registered as
 * `"default"` for that kind), and `config` is opaque data passed through to
 * the factory unchanged.
 */
export interface GrainFacetField {
  fieldName: string;
  kind: string;
  providerName?: string;
  config?: unknown;
}

// Per-instance registry populated by the decorator's initializer during
// construction; the runtime reads it to inject each facet before
// `onActivate`. Keyed by instance (a WeakMap) to avoid leaning on Symbol.metadata.
const registry = createFieldRegistry<GrainFacetField>();

export const registerGrainFacetField = registry.register;
export const getGrainFacetFields = registry.getFields;
