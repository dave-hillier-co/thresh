import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { DirectoryPeer } from "@thresh/directory/directory-peer";
import type { LocalDirectoryPartition } from "@thresh/directory/local-directory-partition";

/**
 * Reaches peer directory partitions by direct call, for silos sharing a process
 * (local dev clusters and multi-silo tests). The transport-backed peer for
 * genuine multi-process clusters arrives with Kubernetes hosting (Phase 3).
 */
export class InProcessDirectoryPeer implements DirectoryPeer {
  constructor(
    private readonly partitionFor: (silo: SiloAddress) => LocalDirectoryPartition | undefined,
  ) {}

  async lookup(owner: SiloAddress, grainId: GrainId): Promise<GrainAddress | undefined> {
    return this.require(owner).lookup(grainId);
  }

  async register(
    owner: SiloAddress,
    addr: GrainAddress,
    previous?: GrainAddress,
  ): Promise<GrainAddress> {
    return this.require(owner).register(addr, previous);
  }

  async unregister(owner: SiloAddress, addr: GrainAddress): Promise<void> {
    this.require(owner).unregister(addr);
  }

  private require(owner: SiloAddress): LocalDirectoryPartition {
    const partition = this.partitionFor(owner);
    if (partition === undefined) {
      throw new RejectionError(`no directory partition for ${owner.toString()}`, "unknownTarget");
    }
    return partition;
  }
}
