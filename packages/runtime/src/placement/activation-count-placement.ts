import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";

/**
 * Power-of-k load balancing: sample k live silos and pick the least loaded by
 * activation count. Balances load without a global coordinator.
 */
export class ActivationCountPlacement implements PlacementStrategy {
  constructor(private readonly k: number = 2) {}

  choose(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    if (candidates.length === 0) throw new Error("no placement candidates");
    const load = ctx.activationCount ?? (() => 0);
    const sample = this.sample(candidates, ctx.random ?? Math.random);
    return sample.reduce((best, c) => (load(c) < load(best) ? c : best));
  }

  private sample(candidates: readonly SiloAddress[], random: () => number): SiloAddress[] {
    const pool = [...candidates];
    const take = Math.min(this.k, pool.length);
    const picked: SiloAddress[] = [];
    for (let i = 0; i < take; i++) {
      const idx = Math.floor(random() * pool.length) % pool.length;
      picked.push(pool.splice(idx, 1)[0]!);
    }
    return picked;
  }
}
