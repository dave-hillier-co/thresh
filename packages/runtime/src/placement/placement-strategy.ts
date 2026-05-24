import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";

/** Information a placement strategy may consult when choosing a silo. */
export interface PlacementContext {
  localSilo: SiloAddress;
  /** Current activation count on a silo, for load-aware strategies. */
  activationCount?: (silo: SiloAddress) => number;
  /** Injectable RNG so placement is deterministic in tests. */
  random?: () => number;
}

/** Chooses which live silo should host a new activation, over the candidate set. */
export interface PlacementStrategy {
  choose(
    grainType: GrainType,
    candidates: readonly SiloAddress[],
    context: PlacementContext,
  ): SiloAddress;
}

export function pickRandom(candidates: readonly SiloAddress[], random: () => number): SiloAddress {
  if (candidates.length === 0) throw new Error("no placement candidates");
  return candidates[Math.floor(random() * candidates.length) % candidates.length]!;
}
