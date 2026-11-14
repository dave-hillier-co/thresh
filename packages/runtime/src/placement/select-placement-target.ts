import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import { withFilterPlacementCandidatesSpan } from "@thresh/observability/activation-tracing";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@thresh/runtime/placement/placement-strategy";

export interface PlacementSelection {
  grainType: GrainType;
  /** Every silo that could host the grain (the active membership). */
  candidates: readonly SiloAddress[];
  /** Silos that must not be chosen, e.g. the silo a migrating activation is leaving. */
  exclude?: readonly SiloAddress[];
  /** The grain's stacked placement filters, applied in order. */
  filters: readonly PlacementFilter[];
  strategy: PlacementStrategy;
  /** The full placement context: peer counts, silo metadata, overload flags, grain id, RNG. */
  context: PlacementContext;
  /** Version-aware pre-filter; the eligible set is kept when it returns nothing. */
  versionFilter?: (candidates: readonly SiloAddress[]) => Promise<readonly SiloAddress[]>;
  /**
   * Directed placement (Orleans `IPlacementDirector.GetPlacementHint`): resolves a hint against
   * the candidates that survived exclusion, filters and version narrowing, so a hint naming a
   * silo those removed is never obeyed.
   */
  hint?: (candidates: readonly SiloAddress[]) => SiloAddress | undefined;
}

/**
 * The one placement pipeline (Orleans `PlacementService`): used for a new activation and for a
 * migrating one. Exclusion, then the stacked filters, then version narrowing, then the hint,
 * then the grain's strategy. Returns `undefined` when exclusion and filtering leave no silo.
 */
export async function selectPlacementTarget(
  selection: PlacementSelection,
): Promise<SiloAddress | undefined> {
  const { grainType, filters, context } = selection;
  const excluded = selection.exclude ?? [];
  let candidates: readonly SiloAddress[] =
    excluded.length === 0
      ? selection.candidates
      : selection.candidates.filter((c) => !excluded.some((e) => e.equals(c)));
  for (const filter of filters) {
    candidates = withFilterPlacementCandidatesSpan({ filterType: filter.constructor.name }, () =>
      filter.filter(grainType, candidates, context),
    );
  }
  if (candidates.length === 0) return undefined;
  if (selection.versionFilter !== undefined) {
    const compatible = await selection.versionFilter(candidates);
    if (compatible.length > 0) candidates = compatible;
  }
  return selection.hint?.(candidates) ?? selection.strategy.choose(grainType, candidates, context);
}
