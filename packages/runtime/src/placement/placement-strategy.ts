import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";

/** Per-silo resource signals for load-aware placement; extensible with cpu/memory later. */
export interface SiloResourceStats {
  activationCount?: number;
}

/** Information a placement strategy may consult when choosing a silo. */
export interface PlacementContext {
  localSilo: SiloAddress;
  /** Current activation count on a silo, for load-aware strategies. */
  activationCount?: (silo: SiloAddress) => number;
  /** Static metadata a silo advertises via membership (role, zone, ...), for metadata filters. */
  siloMetadata?: (silo: SiloAddress) => Readonly<Record<string, string>> | undefined;
  /** Resource signals for resource-optimized placement; local from the catalog, remote from membership. */
  resourceStats?: (silo: SiloAddress) => SiloResourceStats | undefined;
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
