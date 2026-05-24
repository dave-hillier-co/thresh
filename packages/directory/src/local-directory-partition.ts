import { grainAddressEquals, type GrainAddress } from "@tsva/core/grain-address";
import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";

/**
 * The authoritative in-memory store for the grains whose hash lands in this
 * silo's ring range. Holds the compare-and-set registration logic; the
 * distributed directory routes owned-elsewhere operations here over the
 * transport.
 */
export class LocalDirectoryPartition {
  private readonly entries = new Map<string, GrainAddress>();

  get size(): number {
    return this.entries.size;
  }

  lookup(grainId: GrainId): GrainAddress | undefined {
    return this.entries.get(grainId.toString());
  }

  /**
   * Register `addr`. If no entry exists, `addr` wins. If one exists, the caller
   * only wins by passing the exact `previous` entry it expected to replace
   * (a known-stale entry after a move); otherwise the existing winner is
   * returned and the caller must defer to it.
   */
  register(addr: GrainAddress, previous?: GrainAddress): GrainAddress {
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (existing === undefined) {
      this.entries.set(key, addr);
      return addr;
    }
    if (previous !== undefined && grainAddressEquals(existing, previous)) {
      this.entries.set(key, addr);
      return addr;
    }
    return existing;
  }

  unregister(addr: GrainAddress): void {
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (existing !== undefined && grainAddressEquals(existing, addr)) {
      this.entries.delete(key);
    }
  }

  unregisterSilo(silo: SiloAddress): void {
    for (const [key, entry] of this.entries) {
      if (entry.silo.equals(silo)) this.entries.delete(key);
    }
  }
}
