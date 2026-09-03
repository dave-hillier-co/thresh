import { grainAddressEquals, type GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";

/**
 * The authoritative in-memory store for the grains whose hash lands in this
 * silo's ring range. Holds the compare-and-set registration logic; the
 * distributed directory routes owned-elsewhere operations here over the
 * transport.
 *
 * Serializing registration against an in-flight join/handoff recovery pull is
 * the caller's job, not this class's: `ClusterNode` gates every directory
 * operation on its own recovery barrier (`awaitRecovered`, which awaits the
 * whole in-flight recovery promise before the operation reaches this
 * partition), so by the time `register` ever runs here recovery has already
 * either finished or not started. This partition used to also carry a
 * per-call recovery-version gate for the same purpose; it was removed as dead
 * code once every caller was confirmed to funnel through that barrier first.
 */
export class LocalDirectoryPartition {
  private readonly entries = new Map<string, GrainAddress>();
  private readonly isSiloLive: (silo: SiloAddress) => boolean;

  /**
   * `isSiloLive` is consulted on lookup to mirror Orleans' `LookupCore` /
   * `IsSiloDead` path: an entry whose host silo has fallen out of the live
   * membership view is treated as a miss rather than returned as a stale
   * pointer. The cluster wires the current membership snapshot through; tests
   * and in-process callers without a membership view can default to assuming
   * every silo is live.
   */
  constructor(isSiloLive: (silo: SiloAddress) => boolean = () => true) {
    this.isSiloLive = isSiloLive;
  }

  get size(): number {
    return this.entries.size;
  }

  lookup(grainId: GrainId): GrainAddress | undefined {
    const existing = this.entries.get(grainId.toString());
    return existing && this.isSiloLive(existing.silo) ? existing : undefined;
  }

  /**
   * Register `addr`. If no entry exists, `addr` wins outright — a caller
   * racing an in-flight join/handoff recovery for this range is never in
   * play here: `ClusterNode` awaits its recovery barrier before any register
   * reaches this partition, so recovery has always already settled by this
   * point (see the class doc comment). If an entry exists but its host silo
   * has fallen out of the live membership view, it is a dangling pointer to a
   * silo that will never come back — `addr` wins outright, mirroring Orleans
   * `RegisterCore`'s dead-silo overwrite. Otherwise the caller only wins by
   * passing the exact `previous` entry it expected to replace (a known-stale
   * entry after a move); the existing winner is returned and the caller must
   * defer to it.
   */
  register(addr: GrainAddress, previous?: GrainAddress): GrainAddress {
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (existing === undefined) {
      this.entries.set(key, addr);
      return addr;
    }
    if (!this.isSiloLive(existing.silo)) {
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

  /**
   * Reconcile the partition against a new ring in a single pass: classify each
   * entry as kept, dropped (its host silo left — the grain is gone) or handed
   * off (the new ring assigns its range to another live silo). Dropped and
   * handed-off entries leave the live partition; the handed-off ones are
   * returned so the caller can retain them for a successor to pull.
   */
  drain(classify: (entry: GrainAddress) => "keep" | "drop" | "handoff"): GrainAddress[] {
    const handoff: GrainAddress[] = [];
    for (const [key, entry] of this.entries) {
      const decision = classify(entry);
      if (decision === "keep") continue;
      if (decision === "handoff") handoff.push(entry);
      this.entries.delete(key);
    }
    return handoff;
  }

  /**
   * Adopt a batch of entries recovered from a previous owner: register-if-absent
   * each one this silo still owns under the current ring. Never overwrites a
   * fresher entry (a concurrent reactivation wins via the CAS), and skips any
   * entry whose range has since moved to another silo — the versioned guard that
   * keeps a partition from ever mixing two ring topologies.
   */
  acceptHandoff(
    entries: readonly GrainAddress[],
    isOwnedHere: (entry: GrainAddress) => boolean,
  ): void {
    for (const entry of entries) {
      if (!isOwnedHere(entry)) continue;
      this.register(entry);
    }
  }

  /** The live entries matching `predicate` — used to serve a range-recovery pull. */
  entriesIn(predicate: (entry: GrainAddress) => boolean): GrainAddress[] {
    const matches: GrainAddress[] = [];
    for (const entry of this.entries.values()) {
      if (predicate(entry)) matches.push(entry);
    }
    return matches;
  }
}
