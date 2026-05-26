import type { GrainMetadata, PlacementFilterDescriptor } from "@tsva/core/grain-metadata";
import { ActivationCountPlacement } from "@tsva/runtime/placement/activation-count-placement";
import { MetadataMatchFilter } from "@tsva/runtime/placement/metadata-match-filter";
import type { PlacementFilter } from "@tsva/runtime/placement/placement-filter";
import { PreferLocalPlacement } from "@tsva/runtime/placement/prefer-local-placement";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";
import { ResourceOptimizedPlacement } from "@tsva/runtime/placement/resource-optimized-placement";
import type { PlacementStrategy } from "@tsva/runtime/placement/placement-strategy";
import { SiloRoleBasedPlacement } from "@tsva/runtime/placement/silo-role-based-placement";
import { StatelessWorkerPlacement } from "@tsva/runtime/placement/stateless-worker-placement";

/** Resolve the placement strategy a grain type's metadata selects. */
export function placementStrategyFor(metadata: GrainMetadata): PlacementStrategy {
  if (metadata.options.stateless) return new StatelessWorkerPlacement();
  switch (metadata.options.placement) {
    case "preferLocal":
      return new PreferLocalPlacement();
    case "activationCount":
      return new ActivationCountPlacement();
    case "siloRole": {
      const role = metadata.options.role;
      if (role === undefined)
        throw new Error(`${metadata.grainType}: placement "siloRole" requires options.role`);
      return new SiloRoleBasedPlacement(role);
    }
    case "resourceOptimized":
      return new ResourceOptimizedPlacement();
    case "random":
    case undefined:
    default:
      return new RandomPlacement();
  }
}

/**
 * Resolve the placement filters a grain type declares, applied before the strategy
 * runs. Filters are no-ops for stateless workers (always local, never registered),
 * so none are returned for them.
 */
export function placementFiltersFor(metadata: GrainMetadata): readonly PlacementFilter[] {
  if (metadata.options.stateless) return [];
  const descriptors = metadata.options.placementFilters ?? [];
  return descriptors.map(toFilter);
}

function toFilter(descriptor: PlacementFilterDescriptor): PlacementFilter {
  switch (descriptor.kind) {
    case "metadataMatch":
      return new MetadataMatchFilter(descriptor.match);
  }
}
