import { describe, expect, it } from "vitest";
import { newActivationId } from "@thresh/core/activation-id";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { LocationCache } from "@thresh/directory/location-cache";

const siloA = new SiloAddress("silo-A", "ua", "a:1");
const siloB = new SiloAddress("silo-B", "ub", "b:1");
const addr = (key: string, silo: SiloAddress, activationId = newActivationId()): GrainAddress => ({
  grainId: new GrainId("Counter", key),
  silo,
  activationId,
});

describe("LocationCache", () => {
  it("serves a cached hit without a directory round-trip, and re-resolves after invalidation", () => {
    const cache = new LocationCache();
    // The directory behind the cache, reduced to what this test reads of it.
    const dir = new Map<string, GrainAddress>();
    const a = addr("x", siloA);
    dir.set(a.grainId.toString(), a);

    let lookups = 0;
    const resolve = (grainId: GrainId): GrainAddress | undefined => {
      const hit = cache.get(grainId);
      if (hit !== undefined) return hit;
      lookups++;
      const found = dir.get(grainId.toString());
      if (found !== undefined) cache.put(found);
      return found;
    };

    expect(resolve(a.grainId)).toEqual(a);
    expect(resolve(a.grainId)).toEqual(a);
    expect(lookups).toBe(1); // second call served from cache

    cache.invalidate(a.grainId); // e.g. after a rejected send
    expect(resolve(a.grainId)).toEqual(a);
    expect(lookups).toBe(2); // re-resolved through the directory
  });

  it("drops cached entries pointing at a departed silo", () => {
    const cache = new LocationCache();
    cache.put(addr("x", siloA));
    cache.put(addr("y", siloB));
    cache.invalidateSilo(siloA);
    expect(cache.get(new GrainId("Counter", "x"))).toBeUndefined();
    expect(cache.get(new GrainId("Counter", "y"))).toBeDefined();
  });
});
