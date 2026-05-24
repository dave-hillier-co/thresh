import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";

/**
 * Stateless-worker grains keep a local pool of interchangeable activations on
 * each silo, so calls always resolve locally and are never directory-registered.
 */
export class StatelessWorkerPlacement implements PlacementStrategy {
  choose(
    _grainType: GrainType,
    _candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    return ctx.localSilo;
  }
}
