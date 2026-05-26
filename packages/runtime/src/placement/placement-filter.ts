import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { PlacementContext } from "@tsva/runtime/placement/placement-strategy";

/**
 * Prunes the candidate silos by metadata before a placement strategy chooses
 * among them (Orleans `PlacementFilterStrategy`). Filters compose: the output of
 * one is the input to the next.
 */
export interface PlacementFilter {
  filter(
    grainType: GrainType,
    candidates: readonly SiloAddress[],
    context: PlacementContext,
  ): readonly SiloAddress[];
}
