import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { LocationCache } from "@thresh/directory/location-cache";

const grainId = new GrainId("Counter", "k");
const siloB = new SiloAddress("silo-b", "uid-b", "silo-b:1");
const siloC = new SiloAddress("silo-c", "uid-c", "silo-c:1");

// Orleans' cache-invalidation header names the stale ADDRESS, and applying it
// removes the cached entry only if it still matches
// (`CachedGrainLocator.InvalidateCache(GrainAddress)`): a reply carrying a
// forward's stale hint must not evict an entry that has since been refreshed
// to the grain's real location (issue #110).
describe("LocationCache.invalidate with a stale silo", () => {
  it("evicts the entry when it still points at that silo", () => {
    const cache = new LocationCache();
    cache.put({ grainId, silo: siloB, activationId: "a1" });

    cache.invalidate(grainId, siloB);

    expect(cache.stats.size).toBe(0);
  });

  it("keeps an entry that has since been refreshed to another silo", () => {
    const cache = new LocationCache();
    cache.put({ grainId, silo: siloC, activationId: "a2" });

    cache.invalidate(grainId, siloB);

    expect(cache.get(grainId)?.silo).toBe(siloC);
  });
});
