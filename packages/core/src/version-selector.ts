/**
 * Given the versions a cluster currently hosts that are *compatible* with a
 * request, chooses which of them placement may target — Orleans'
 * `VersionSelectorStrategy`. The director ([version-compatibility](./version-compatibility.ts))
 * decides compatibility; the selector narrows the surviving versions.
 */
export type VersionSelectorStrategy = (
  requested: number,
  compatible: readonly number[],
) => readonly number[];

export type VersionSelectorKind = "latest" | "all" | "minimum";

/** Default: target only the highest compatible version on offer. */
export const latest: VersionSelectorStrategy = (_requested, compatible) =>
  compatible.length === 0 ? [] : [Math.max(...compatible)];

/** Target any compatible version (widest candidate set). */
export const all: VersionSelectorStrategy = (_requested, compatible) => compatible;

/** Target only the lowest compatible version (drains older silos last). */
export const minimum: VersionSelectorStrategy = (_requested, compatible) =>
  compatible.length === 0 ? [] : [Math.min(...compatible)];

export function versionSelector(kind: VersionSelectorKind): VersionSelectorStrategy {
  switch (kind) {
    case "all":
      return all;
    case "minimum":
      return minimum;
    case "latest":
    default:
      return latest;
  }
}
