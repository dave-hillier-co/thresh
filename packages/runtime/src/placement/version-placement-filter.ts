import { supportedVersion, type SiloManifest } from "@tsva/core/grain-manifest";
import type { CompatibilityDirector } from "@tsva/core/version-compatibility";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { VersionSelectorStrategy } from "@tsva/core/version-selector";

export interface VersionFilterDeps {
  director: CompatibilityDirector;
  selector: VersionSelectorStrategy;
  /** Resolves a silo's advertised manifest (local synchronously, peers cached). */
  getManifest: (silo: SiloAddress) => Promise<SiloManifest>;
}

/**
 * Prune placement candidates to the silos that can serve a request for
 * `interfaceId` at `requested` version: keep silos whose implemented version the
 * compatibility director accepts, then narrow to the versions the selector picks
 * (Orleans' version-aware placement). Returns `[]` when none qualify — the caller
 * decides whether to fall back to the full candidate set.
 */
export async function filterByVersion(
  interfaceId: number,
  requested: number,
  candidates: readonly SiloAddress[],
  deps: VersionFilterDeps,
): Promise<readonly SiloAddress[]> {
  const manifests = await Promise.all(candidates.map((silo) => deps.getManifest(silo)));

  const hosting: Array<{ silo: SiloAddress; version: number }> = [];
  for (let i = 0; i < candidates.length; i++) {
    const version = supportedVersion(manifests[i]!, interfaceId);
    if (version !== undefined && deps.director(requested, version)) {
      hosting.push({ silo: candidates[i]!, version });
    }
  }
  if (hosting.length === 0) return [];

  const selected = new Set(
    deps.selector(
      requested,
      hosting.map((h) => h.version),
    ),
  );
  return hosting.filter((h) => selected.has(h.version)).map((h) => h.silo);
}
