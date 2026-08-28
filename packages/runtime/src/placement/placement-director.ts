import type { GrainMetadata, PlacementFilterDescriptor } from "@thresh/core/grain-metadata";
import { ActivationCountPlacement } from "@thresh/runtime/placement/activation-count-placement";
import { MetadataMatchFilter } from "@thresh/runtime/placement/metadata-match-filter";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import type { PlacementFilterRegistry } from "@thresh/runtime/placement/placement-filter-registry";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { RandomPlacement } from "@thresh/runtime/placement/random-placement";
import { ResourceOptimizedPlacement } from "@thresh/runtime/placement/resource-optimized-placement";
import type { PlacementStrategy } from "@thresh/runtime/placement/placement-strategy";
import type { PlacementStrategyRegistry } from "@thresh/runtime/placement/placement-strategy-registry";
import {
  PreferredMatchSiloMetadataPlacementFilterDirector,
  RequiredMatchSiloMetadataPlacementFilterDirector,
} from "@thresh/runtime/placement/silo-metadata-match-filters";
import { SiloRoleBasedPlacement } from "@thresh/runtime/placement/silo-role-based-placement";
import { StatelessWorkerPlacement } from "@thresh/runtime/placement/stateless-worker-placement";

/**
 * Resolve the placement strategy a grain type's metadata selects. A `"custom"` placement
 * resolves its named strategy from `registry` (populated by a silo builder's
 * `addPlacementStrategy(name, strategy)`); throws if it names none, or names one no registry
 * (or an unpopulated one) has. Stateless-worker placement wins over everything, as in Orleans.
 */
export function placementStrategyFor(
  metadata: GrainMetadata,
  registry?: PlacementStrategyRegistry,
): PlacementStrategy {
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
    case "custom": {
      // Resolved by name from the silo builder's `addPlacementStrategy` registry, exactly as a
      // `"custom"` placement-filter descriptor resolves from `addPlacementFilter`. Orleans
      // reaches the equivalent through DI on the grain's `[PlacementStrategy]` attribute.
      const name = metadata.options.strategy;
      if (name === undefined)
        throw new Error(`${metadata.grainType}: placement "custom" requires options.strategy`);
      if (registry === undefined)
        throw new Error(
          `${metadata.grainType}: no placement strategy registered: "${name}" (none are; did the silo builder call addPlacementStrategy?)`,
        );
      return registry.resolve(name);
    }
    case "random":
    case undefined:
    default:
      return new RandomPlacement();
  }
}

/**
 * Resolve the placement filters a grain type declares, applied before the strategy
 * runs, composed in ascending `order` (Orleans' stackable
 * `[XyzPlacementFilter(order)]` attributes — the output of one filter feeds the
 * next). Filters are no-ops for stateless workers (always local, never
 * registered), so none are returned for them. A `"custom"` descriptor resolves
 * its named director from `registry` (populated by a silo builder's
 * `addPlacementFilter(name, director)`); throws if two descriptors on the same
 * grain declare the same `order`, or a `"custom"` descriptor names a director
 * no registry (or an unpopulated one) has (Orleans `InvalidOperationException`
 * / `KeyNotFoundException`, both surfaced here as a plain `Error`).
 */
export function placementFiltersFor(
  metadata: GrainMetadata,
  registry?: PlacementFilterRegistry,
): readonly PlacementFilter[] {
  if (metadata.options.stateless) return [];
  const descriptors = metadata.options.placementFilters ?? [];
  if (descriptors.length > 1) {
    const orders = descriptors.map((d) => d.order ?? 0);
    if (new Set(orders).size !== orders.length) {
      throw new Error(
        `${metadata.grainType}: placement filters have duplicate order values; ` +
          `order must be unique when more than one filter is applied`,
      );
    }
  }
  const ordered = [...descriptors].sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
  return ordered.map((descriptor) => toFilter(descriptor, metadata, registry));
}

function toFilter(
  descriptor: PlacementFilterDescriptor,
  metadata: GrainMetadata,
  registry: PlacementFilterRegistry | undefined,
): PlacementFilter {
  switch (descriptor.kind) {
    case "metadataMatch":
      return new MetadataMatchFilter(descriptor.match);
    case "requiredMatchSiloMetadata":
      return new RequiredMatchSiloMetadataPlacementFilterDirector(descriptor.keys);
    case "preferredMatchSiloMetadata":
      return new PreferredMatchSiloMetadataPlacementFilterDirector(
        descriptor.keys,
        descriptor.minCandidates,
      );
    case "custom":
      if (registry === undefined) {
        throw new Error(
          `${metadata.grainType}: no placement-filter registry configured for custom filter "${descriptor.name}"`,
        );
      }
      return registry.resolve(descriptor.name);
  }
}
