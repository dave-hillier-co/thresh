import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";

/**
 * Named custom placement-filter directors registered on a silo builder
 * (Orleans `IServiceCollection.AddPlacementFilter<TStrategy, TDirector>`,
 * generalized: this framework has no separate strategy-vs-director DI split,
 * so a director *is* a `PlacementFilter`, registered directly under the name
 * a grain's `placementFilters` descriptor references). Mirrors
 * `GrainFacetRegistry`'s "named factory, resolved by name at bind time"
 * shape, generalized from grain facets to placement filters.
 */
export class PlacementFilterRegistry {
  private readonly directors = new Map<string, PlacementFilter>();

  add(name: string, director: PlacementFilter): this {
    this.directors.set(name, director);
    return this;
  }

  resolve(name: string): PlacementFilter {
    const director = this.directors.get(name);
    if (director === undefined) {
      throw new Error(`no placement-filter director registered: "${name}"`);
    }
    return director;
  }
}
