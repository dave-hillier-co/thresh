import { PLACEMENT_HINT_KEY } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";

export { PLACEMENT_HINT_KEY };

/**
 * Resolve a raw `SiloAddress.toString()` hint value against a set of live
 * candidates — Orleans `IPlacementDirector.GetPlacementHint`. Only honoured
 * when it matches one of `candidates`, so a hint naming a dead or unknown
 * silo falls through to the normal placement strategy.
 */
export function resolvePlacementHintValue(
  hint: string | undefined,
  candidates: readonly SiloAddress[],
): SiloAddress | undefined {
  if (hint === undefined) return undefined;
  return candidates.find((c) => c.toString() === hint);
}

/**
 * Resolve the `PLACEMENT_HINT_KEY` `RequestContext` header (if present) against
 * a set of live candidates. The header carries `SiloAddress.toString()` (set
 * by a caller via `RequestContext.set(PLACEMENT_HINT_KEY, silo.toString())`).
 */
export function resolvePlacementHint(
  headers: Record<string, string> | undefined,
  candidates: readonly SiloAddress[],
): SiloAddress | undefined {
  return resolvePlacementHintValue(headers?.[PLACEMENT_HINT_KEY], candidates);
}
