import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";
import {
  pickRandom,
  type PlacementContext,
  type PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";

/** Place on the calling silo when it is a candidate, else fall back to random. */
export class PreferLocalPlacement implements PlacementStrategy {
  choose(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    const local = candidates.find((c) => c.equals(ctx.localSilo));
    return local ?? pickRandom(candidates, ctx.random ?? Math.random);
  }
}
