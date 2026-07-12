import type { GrainId } from "@tsva/core/grain-id";

export const DEFAULT_FACET_PROVIDER = "default";

/**
 * Builds a bound facet instance from a `@grainFacet` field's `config`. May
 * return the facet directly or a promise (mirrors Orleans' `IExampleStorage`
 * factories, some of which run an async load step while binding).
 */
export type GrainFacetFactory = (config: unknown, grainId: GrainId) => unknown | Promise<unknown>;

/**
 * Named, per-kind factories for third-party grain facets, registered on a
 * silo builder via `addGrainFacetFactory(kind, name, factory)` and resolved by
 * `@grainFacet(kind, name)` field bindings. Mirrors `StorageRegistry`,
 * generalized from "named storage provider" to "named factory for any facet
 * family" — `kind` namespaces unrelated facet plugins (e.g. a third party's
 * `"exampleStorage"`) so their provider names don't collide. A factory
 * registered under `"default"` for a kind is the fallback for a `@grainFacet`
 * field that names no explicit provider (Orleans' `UseAsDefaultExampleStorage`).
 */
export class GrainFacetRegistry {
  private readonly kinds = new Map<string, Map<string, GrainFacetFactory>>();

  add(kind: string, name: string, factory: GrainFacetFactory): this {
    let byName = this.kinds.get(kind);
    if (byName === undefined) {
      byName = new Map();
      this.kinds.set(kind, byName);
    }
    byName.set(name, factory);
    return this;
  }

  resolve(kind: string, name: string = DEFAULT_FACET_PROVIDER): GrainFacetFactory {
    const factory = this.kinds.get(kind)?.get(name);
    if (factory === undefined) throw new Error(`no "${kind}" facet factory registered: ${name}`);
    return factory;
  }
}
