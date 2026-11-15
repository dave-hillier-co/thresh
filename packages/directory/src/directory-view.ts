import { memberSilos, ringSilos, type MembershipSnapshot } from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import type { RingRange } from "@thresh/directory/ring-range";

/**
 * What the directory needs of one membership view (Orleans `DirectoryMembershipSnapshot`),
 * derived from it and from nothing else, so two silos holding the same view agree on every
 * answer: its version, its ring (`ringSilos`: the active members, or the joining ones while none
 * is active), its members (`memberSilos`: every listed silo, whatever its status), the owner of a
 * hash and the ranges of a silo.
 */
export class DirectoryView {
  /** The view a partition holds before it has applied one (Orleans `MembershipVersion.MinValue`). */
  static readonly initial = new DirectoryView(-1, [], []);

  readonly ring: ConsistentHashRing;
  private readonly ranges = new Map<string, RingRange[]>();

  constructor(
    readonly version: number,
    ring: readonly SiloAddress[],
    readonly members: readonly SiloAddress[],
  ) {
    this.ring = new ConsistentHashRing(ring);
  }

  static of(snapshot: MembershipSnapshot): DirectoryView {
    return new DirectoryView(snapshot.version, ringSilos(snapshot), memberSilos(snapshot));
  }

  /** The silos that own ranges in this view. */
  get ringSilos(): readonly SiloAddress[] {
    return this.ring.silos();
  }

  /** The owner of `hash`, or `undefined` when the ring is empty. */
  ownerOf(hash: number): SiloAddress | undefined {
    return this.ring.isEmpty ? undefined : this.ring.owner(hash);
  }

  /** Whether `silo` owns `hash` in this view. */
  owns(silo: SiloAddress, hash: number): boolean {
    return this.ownerOf(hash)?.equals(silo) === true;
  }

  /** The ranges `silo` owns in this view: none unless it is in the ring. */
  rangesOf(silo: SiloAddress): readonly RingRange[] {
    const key = silo.toString();
    let ranges = this.ranges.get(key);
    if (ranges === undefined) {
      ranges = this.ring.arcsOf(silo);
      this.ranges.set(key, ranges);
    }
    return ranges;
  }

  isMember(silo: SiloAddress): boolean {
    return this.members.some((m) => m.equals(silo));
  }

  inRing(silo: SiloAddress): boolean {
    return this.ring.silos().some((s) => s.equals(silo));
  }
}
