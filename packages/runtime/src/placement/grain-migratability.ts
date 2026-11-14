import { isClientGrainType } from "@thresh/core/client-grain-id";
import type { GrainMetadata } from "@thresh/core/grain-metadata";
import type { GrainType } from "@thresh/core/grain-type";

/** The movers that can be barred from a grain type (Orleans `ImmovableKind`). */
export type MigrationInitiator = "rebalancer" | "repartitioner";

/**
 * Whether the rebalancer or repartitioner may move a grain of `grainType` (Orleans
 * `GrainMigratabilityChecker.IsMigratable`). Never movable: client grains, system grains (the
 * reserved `$`-prefixed types this runtime registers for its own targets, the analogue of
 * Orleans' system targets and grain services), stateless workers, grains marked immovable for
 * this mover or for any, and, as in Orleans, a type this silo has no registration for.
 */
export function isMigratable(
  grainType: GrainType,
  initiator: MigrationInitiator,
  metadata: GrainMetadata | undefined,
): boolean {
  if (isClientGrainType(grainType) || isSystemGrainType(grainType)) return false;
  if (metadata === undefined) return false;
  if (metadata.options.stateless === true) return false;
  const immovable = metadata.options.immovable;
  return immovable !== "any" && immovable !== initiator;
}

/** The runtime's own reserved grain types (`$directory`, `$management`, `$system.*`, ...). */
export function isSystemGrainType(grainType: GrainType): boolean {
  return grainType.startsWith("$");
}
