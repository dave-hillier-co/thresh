import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@thresh/runtime/placement/placement-strategy";

/**
 * Pick where a migrating activation should go. If a `directed` silo is named and
 * it is a live candidate other than the local silo, honour it (directed
 * placement). Otherwise let the grain's placement strategy choose among the other
 * live silos. Returns `undefined` when there is nowhere to move to (the local
 * silo is the only candidate), so the caller falls back to plain deactivation.
 */
export function chooseMigrationTarget(
  grainType: GrainType,
  directed: SiloAddress | undefined,
  candidates: readonly SiloAddress[],
  strategy: PlacementStrategy,
  context: PlacementContext,
): SiloAddress | undefined {
  if (
    directed !== undefined &&
    !directed.equals(context.localSilo) &&
    candidates.some((c) => c.equals(directed))
  ) {
    return directed;
  }
  const others = candidates.filter((c) => !c.equals(context.localSilo));
  if (others.length === 0) return undefined;
  return strategy.choose(grainType, others, context);
}
