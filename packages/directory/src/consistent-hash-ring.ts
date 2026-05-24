import type { GrainId } from "@tsva/core/grain-id";
import { stableHash32 } from "@tsva/core/hash";
import type { SiloAddress } from "@tsva/core/silo-address";

interface RingNode {
  hash: number;
  silo: SiloAddress;
}

// Virtual nodes per silo. More vnodes give tighter balance and a smaller
// reshuffle on membership change, at the cost of a larger sorted ring.
const DEFAULT_VNODES = 100;

/**
 * Consistent-hash ring over the live silo set. Each silo owns several virtual
 * nodes so ownership is balanced and a join/leave only reshuffles a fraction of
 * the space. Built deterministically from `podName`s, so every silo computing
 * the ring from the same membership view agrees on owners without coordination.
 */
export class ConsistentHashRing {
  private readonly nodes: RingNode[];
  private readonly members: readonly SiloAddress[];

  constructor(silos: readonly SiloAddress[], vnodes: number = DEFAULT_VNODES) {
    this.members = [...silos];
    const nodes: RingNode[] = [];
    for (const silo of silos) {
      for (let v = 0; v < vnodes; v++) {
        nodes.push({ hash: stableHash32(`${silo.ringKey}#${v}`), silo });
      }
    }
    nodes.sort((a, b) => a.hash - b.hash || (a.silo.ringKey < b.silo.ringKey ? -1 : 1));
    this.nodes = nodes;
  }

  get isEmpty(): boolean {
    return this.nodes.length === 0;
  }

  /** The distinct silos the ring was built from. */
  silos(): readonly SiloAddress[] {
    return this.members;
  }

  ownerOf(grainId: GrainId): SiloAddress {
    return this.owner(grainId.getUniformHashCode());
  }

  owner(hash: number): SiloAddress {
    if (this.nodes.length === 0) throw new Error("cannot resolve owner on an empty ring");
    return this.nodes[this.firstAtOrAfter(hash) % this.nodes.length]!.silo;
  }

  /**
   * The half-open `[begin, end)` hash arcs this silo owns. Each arc runs from one
   * vnode to the next; arcs for all silos tile `[0, 2^32)` exactly once, so every
   * silo computing the ring from the same view agrees on a single owner per hash.
   * Used to distribute reminder ownership across the cluster (a silo fires only
   * the reminders whose grain hashes into one of its arcs).
   */
  rangesFor(silo: SiloAddress): Array<[number, number]> {
    const ranges: Array<[number, number]> = [];
    for (let i = 0; i < this.nodes.length; i++) {
      if (!this.nodes[i]!.silo.equals(silo)) continue;
      const begin = this.nodes[i]!.hash;
      const end = this.nodes[(i + 1) % this.nodes.length]!.hash;
      ranges.push([begin, end]);
    }
    return ranges;
  }

  /** Index of the first node whose hash is >= `hash`, wrapping to 0 at the end. */
  private firstAtOrAfter(hash: number): number {
    let lo = 0;
    let hi = this.nodes.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.nodes[mid]!.hash < hash) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}
