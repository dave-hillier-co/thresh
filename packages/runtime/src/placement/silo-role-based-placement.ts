import type { GrainType } from "@tsva/core/grain-type";
import type { SiloAddress } from "@tsva/core/silo-address";
import { roleMatchFilter } from "@tsva/runtime/placement/metadata-match-filter";
import type { PlacementFilter } from "@tsva/runtime/placement/placement-filter";
import {
  pickRandom,
  type PlacementContext,
  type PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";

/**
 * Places the grain on a silo advertising a given role (Orleans
 * `SiloRoleBasedPlacement`). Reuses the role-match filter so role logic lives in
 * one place, then picks randomly among the matches. Throws if no silo has the role.
 */
export class SiloRoleBasedPlacement implements PlacementStrategy {
  private readonly roleFilter: PlacementFilter;

  constructor(private readonly role: string) {
    this.roleFilter = roleMatchFilter(role);
  }

  choose(
    grainType: GrainType,
    candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    const matched = this.roleFilter.filter(grainType, candidates, ctx);
    if (matched.length === 0) throw new Error(`no silo with role "${this.role}"`);
    return pickRandom(matched, ctx.random ?? Math.random);
  }
}
