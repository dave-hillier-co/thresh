import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import {
  pickRandom,
  type PlacementContext,
  type PlacementStrategy,
} from "@thresh/runtime/placement/placement-strategy";

/** The default: pick a random live silo. Cheap and well-distributed. */
export class RandomPlacement implements PlacementStrategy {
  choose(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    return pickRandom(candidates, ctx.random ?? Math.random);
  }
}
