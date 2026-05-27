/**
 * Decides whether a silo that *implements* a given interface version can serve a
 * caller that *requested* another — Orleans' `CompatibilityDirector`. Used to
 * prune placement candidates during a heterogeneous rolling upgrade.
 */
export type CompatibilityDirector = (requested: number, implemented: number) => boolean;

export type CompatibilityKind = "backwardCompatible" | "strict";

/** Default: a newer (or equal) implementation can serve an older request. */
export const backwardCompatible: CompatibilityDirector = (requested, implemented) =>
  implemented >= requested;

/** Exact-match only: the implementation must be the requested version. */
export const strict: CompatibilityDirector = (requested, implemented) => implemented === requested;

export function compatibilityDirector(kind: CompatibilityKind): CompatibilityDirector {
  return kind === "strict" ? strict : backwardCompatible;
}
