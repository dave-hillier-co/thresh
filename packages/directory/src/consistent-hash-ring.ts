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

  constructor(silos: readonly SiloAddress[], vnodes: number = DEFAULT_VNODES) {
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

  ownerOf(grainId: GrainId): SiloAddress {
    return this.owner(grainId.getUniformHashCode());
  }

  owner(hash: number): SiloAddress {
    if (this.nodes.length === 0) throw new Error("cannot resolve owner on an empty ring");
    return this.nodes[this.firstAtOrAfter(hash) % this.nodes.length]!.silo;
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
