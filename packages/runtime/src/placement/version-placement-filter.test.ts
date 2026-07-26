import { describe, expect, it } from "vitest";
import type { SiloManifest } from "@thresh/core/grain-manifest";
import { backwardCompatible, strict } from "@thresh/core/version-compatibility";
import { all, latest, minimum } from "@thresh/core/version-selector";
import { SiloAddress } from "@thresh/core/silo-address";
import { filterByVersion } from "@thresh/runtime/placement/version-placement-filter";

const IFACE = 42;
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);

/** Build a `getManifest` resolver from a silo-index -> implemented-version map. */
function manifests(versions: Record<number, number | undefined>) {
  return (s: SiloAddress): Promise<SiloManifest> => {
    const n = Number(s.podName.split("-")[1]);
    const version = versions[n];
    const entries = version === undefined ? [] : [{ interfaceId: IFACE, version, grainType: "C" }];
    return Promise.resolve({ silo: s, entries });
  };
}

const keys = (silos: readonly SiloAddress[]) => silos.map((s) => s.podName);

describe("filterByVersion", () => {
  const candidates = [silo(0), silo(1), silo(2)];

  it("keeps backward-compatible silos and selects the latest version", async () => {
    // silo-0=v1, silo-1=v2, silo-2=v2; a v2 request keeps the v2 silos.
    const result = await filterByVersion(IFACE, 2, candidates, {
      director: backwardCompatible,
      selector: latest,
      getManifest: manifests({ 0: 1, 1: 2, 2: 2 }),
    });
    expect(keys(result)).toEqual(["silo-1", "silo-2"]);
  });

  it("a v1 request reaches a newer silo under backward compatibility", async () => {
    const result = await filterByVersion(IFACE, 1, [silo(1)], {
      director: backwardCompatible,
      selector: latest,
      getManifest: manifests({ 1: 2 }),
    });
    expect(keys(result)).toEqual(["silo-1"]);
  });

  it("strict compatibility keeps only the exact version", async () => {
    const result = await filterByVersion(IFACE, 2, candidates, {
      director: strict,
      selector: all,
      getManifest: manifests({ 0: 1, 1: 2, 2: 3 }),
    });
    expect(keys(result)).toEqual(["silo-1"]);
  });

  it("minimum selector narrows to the lowest compatible version", async () => {
    const result = await filterByVersion(IFACE, 1, candidates, {
      director: backwardCompatible,
      selector: minimum,
      getManifest: manifests({ 0: 1, 1: 2, 2: 1 }),
    });
    expect(keys(result)).toEqual(["silo-0", "silo-2"]);
  });

  it("returns empty when no silo is compatible (caller falls back)", async () => {
    const result = await filterByVersion(IFACE, 3, candidates, {
      director: backwardCompatible,
      selector: latest,
      getManifest: manifests({ 0: 1, 1: 2, 2: undefined }),
    });
    expect(result).toEqual([]);
  });

  it("excludes silos that do not host the interface at all", async () => {
    const result = await filterByVersion(IFACE, 1, candidates, {
      director: backwardCompatible,
      selector: all,
      getManifest: manifests({ 0: undefined, 1: 1, 2: undefined }),
    });
    expect(keys(result)).toEqual(["silo-1"]);
  });
});
