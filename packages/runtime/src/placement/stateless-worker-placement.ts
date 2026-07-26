import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@thresh/runtime/placement/placement-strategy";

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
