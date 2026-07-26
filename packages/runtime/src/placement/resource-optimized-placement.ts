import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@thresh/runtime/placement/placement-strategy";

/**
 * Places the grain on the least-loaded silo (Orleans `ResourceOptimizedPlacement`).
 * Load is the silo's activation count, read from `resourceStats` (falling back to
 * `activationCount`, then 0). Ties prefer the local silo, keeping placement
 * deterministic and favouring co-location.
 */
export class ResourceOptimizedPlacement implements PlacementStrategy {
  choose(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    if (candidates.length === 0) throw new Error("no placement candidates");
    const load = (silo: SiloAddress): number =>
      ctx.resourceStats?.(silo)?.activationCount ?? ctx.activationCount?.(silo) ?? 0;
    let best = candidates[0]!;
    let bestLoad = load(best);
    for (const silo of candidates.slice(1)) {
      const l = load(silo);
      if (l < bestLoad || (l === bestLoad && silo.equals(ctx.localSilo))) {
        best = silo;
        bestLoad = l;
      }
    }
    return best;
  }
}
