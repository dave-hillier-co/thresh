import type { GrainAddress } from "@tsva/core/grain-address";
import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { ConsistentHashRing } from "@tsva/directory/consistent-hash-ring";
import type { DirectoryPeer } from "@tsva/directory/directory-peer";
import type { GrainDirectory } from "@tsva/directory/grain-directory";
import type { LocalDirectoryPartition } from "@tsva/directory/local-directory-partition";

/**
 * The distributed directory as seen from one silo. The ring (derived from the
 * current membership view) decides which silo owns a grain's entry: owned-here
 * operations hit the local partition; others route to the owner via the peer.
 * Because every silo computes the same ring from the same view, they agree on
 * owners without coordination, so `register` CAS is authoritative.
 */
export class DistributedGrainDirectory implements GrainDirectory {
  constructor(
    private readonly local: SiloAddress,
    private readonly partition: LocalDirectoryPartition,
    private readonly ring: () => ConsistentHashRing,
    private readonly peer: DirectoryPeer,
  ) {}

  async lookup(grainId: GrainId): Promise<GrainAddress | undefined> {
    const owner = this.ring().ownerOf(grainId);
    return owner.equals(this.local)
      ? this.partition.lookup(grainId)
      : this.peer.lookup(owner, grainId);
  }

  async register(addr: GrainAddress, previous?: GrainAddress): Promise<GrainAddress> {
    const owner = this.ring().ownerOf(addr.grainId);
    return owner.equals(this.local)
      ? this.partition.register(addr, previous)
      : this.peer.register(owner, addr, previous);
  }

  async unregister(addr: GrainAddress): Promise<void> {
    const owner = this.ring().ownerOf(addr.grainId);
    if (owner.equals(this.local)) this.partition.unregister(addr);
    else await this.peer.unregister(owner, addr);
  }

  /** Applied locally on every silo when a peer leaves: drop entries pointing at it. */
  async unregisterSilo(silo: SiloAddress): Promise<void> {
    this.partition.unregisterSilo(silo);
  }
}
