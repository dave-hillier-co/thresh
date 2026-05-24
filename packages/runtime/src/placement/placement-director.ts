import type { GrainMetadata } from "@tsva/core/grain-metadata";
import { ActivationCountPlacement } from "@tsva/runtime/placement/activation-count-placement";
import { PreferLocalPlacement } from "@tsva/runtime/placement/prefer-local-placement";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";
import type { PlacementStrategy } from "@tsva/runtime/placement/placement-strategy";
import { StatelessWorkerPlacement } from "@tsva/runtime/placement/stateless-worker-placement";

/** Resolve the placement strategy a grain type's metadata selects. */
export function placementStrategyFor(metadata: GrainMetadata): PlacementStrategy {
  if (metadata.options.stateless) return new StatelessWorkerPlacement();
  switch (metadata.options.placement) {
    case "preferLocal":
      return new PreferLocalPlacement();
    case "activationCount":
      return new ActivationCountPlacement();
    case "random":
    case undefined:
    default:
      return new RandomPlacement();
  }
}
