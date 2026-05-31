import type { GrainAddress } from "@tsva/core/grain-address";
import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";

/**
 * Per-silo read-through cache of grain locations, so most calls skip a
 * directory round-trip. Entries are invalidated when a message to a cached
 * address is rejected, or when the silo they point at leaves the cluster.
 */
export class LocationCache {
  private readonly cache = new Map<string, GrainAddress>();
  private hits = 0;
  private misses = 0;

  /** Cumulative cache hits/misses, for the directory hit-rate metric. */
  get stats(): { hits: number; misses: number; size: number } {
    return { hits: this.hits, misses: this.misses, size: this.cache.size };
  }

  get(grainId: GrainId): GrainAddress | undefined {
    const entry = this.cache.get(grainId.toString());
    if (entry === undefined) this.misses += 1;
    else this.hits += 1;
    return entry;
  }

  put(addr: GrainAddress): void {
    this.cache.set(addr.grainId.toString(), addr);
  }

  invalidate(grainId: GrainId): void {
    this.cache.delete(grainId.toString());
  }

  invalidateSilo(silo: SiloAddress): void {
    for (const [key, entry] of this.cache) {
      if (entry.silo.equals(silo)) this.cache.delete(key);
    }
  }

  clear(): void {
    this.cache.clear();
  }
}
