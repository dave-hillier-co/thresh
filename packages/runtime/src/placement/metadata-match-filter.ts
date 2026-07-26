import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";
import { ROLE_METADATA_KEY } from "@thresh/runtime/placement/silo-metadata";

/**
 * Keeps only candidates whose advertised silo metadata defines every required
 * key/value pair (Orleans `PreferredMatchSiloMetadataPlacementFilter`). A silo
 * with no metadata, or missing any required key, is pruned.
 */
export class MetadataMatchFilter implements PlacementFilter {
  constructor(private readonly required: Readonly<Record<string, string>>) {}

  filter(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    context: PlacementContext,
  ): readonly SiloAddress[] {
    const lookup = context.siloMetadata ?? (() => undefined);
    const required = Object.entries(this.required);
    return candidates.filter((silo) => {
      const meta = lookup(silo);
      return meta !== undefined && required.every(([key, value]) => meta[key] === value);
    });
  }
}

/** A filter that keeps only silos advertising the given role. */
export function roleMatchFilter(role: string): PlacementFilter {
  return new MetadataMatchFilter({ [ROLE_METADATA_KEY]: role });
}
