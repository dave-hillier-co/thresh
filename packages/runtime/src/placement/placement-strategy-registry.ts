import type { PlacementStrategy } from "@thresh/runtime/placement/placement-strategy";

/**
 * Named custom placement strategies registered on a silo builder (Orleans
 * `IPlacementDirector` behind a `PlacementStrategy` attribute, generalized the same way
 * `PlacementFilterRegistry` generalizes placement filters: this framework has no separate
 * strategy-vs-director DI split, so a director *is* a `PlacementStrategy`, registered
 * directly under the name a grain's `{ placement: "custom", strategy }` option references).
 *
 * The filter registry has existed since placement filters landed; this is its counterpart for
 * whole strategies, which until now were a closed set of built-ins.
 */
export class PlacementStrategyRegistry {
  private readonly strategies = new Map<string, PlacementStrategy>();

  add(name: string, strategy: PlacementStrategy): this {
    this.strategies.set(name, strategy);
    return this;
  }

  resolve(name: string): PlacementStrategy {
    const strategy = this.strategies.get(name);
    if (strategy === undefined) {
      throw new Error(`no placement strategy registered: "${name}"`);
    }
    return strategy;
  }
}
