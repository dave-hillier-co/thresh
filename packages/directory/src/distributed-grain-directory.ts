import { RejectionError } from "@tsva/core/errors";
import type { GrainAddress } from "@tsva/core/grain-address";
import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { ConsistentHashRing } from "@tsva/directory/consistent-hash-ring";
import type { DirectoryPeer } from "@tsva/directory/directory-peer";
import type { GrainDirectory } from "@tsva/directory/grain-directory";
import type { LocalDirectoryPartition } from "@tsva/directory/local-directory-partition";

/** Bounded re-resolution attempts when a peer reports our membership view is stale. */
const MAX_STALE_RETRIES = 5;

/**
 * The distributed directory as seen from one silo. The ring (derived from the
 * current membership view) decides which silo owns a grain's entry: owned-here
 * operations hit the local partition; others route to the owner via the peer.
 * Because every silo computes the same ring from the same view, they agree on
 * owners without coordination, so `register` CAS is authoritative.
 *
 * Two membership-change concerns ride on this path. A caller whose ring is stale
 * is told so by the owner (a `staleView` rejection); it then `refresh`es its view
 * and re-resolves the owner. And an owner-local access waits on `onOwnedAccess`
 * so a range still being recovered after a join is not read before its entries
 * have been pulled — closing the reactivation window the old drop-and-rebuild had.
 */
export class DistributedGrainDirectory implements GrainDirectory {
  constructor(
    private readonly local: SiloAddress,
    private readonly partition: LocalDirectoryPartition,
    private readonly ring: () => ConsistentHashRing,
    private readonly peer: DirectoryPeer,
    private readonly refresh: () => void = () => undefined,
    private readonly onOwnedAccess: (grainId: GrainId) => Promise<void> = async () => undefined,
  ) {}

  async lookup(grainId: GrainId): Promise<GrainAddress | undefined> {
    return this.route(
      grainId,
      () => this.partition.lookup(grainId),
      (owner) => this.peer.lookup(owner, grainId),
    );
  }

  async register(addr: GrainAddress, previous?: GrainAddress): Promise<GrainAddress> {
    return this.route(
      addr.grainId,
      () => this.partition.register(addr, previous),
      (owner) => this.peer.register(owner, addr, previous),
    );
  }

  async unregister(addr: GrainAddress): Promise<void> {
    await this.route(
      addr.grainId,
      () => {
        this.partition.unregister(addr);
        return undefined;
      },
      async (owner) => {
        await this.peer.unregister(owner, addr);
      },
    );
  }

  /** Applied locally on every silo when a peer leaves: drop entries pointing at it. */
  async unregisterSilo(silo: SiloAddress): Promise<void> {
    this.partition.unregisterSilo(silo);
  }

  /**
   * Resolve the owner of `grainId` and run the operation there. Owned-here reads
   * wait for any in-flight range recovery first; remote calls re-resolve and
   * retry (bounded) when the owner reports our view is stale.
   */
  private async route<T>(
    grainId: GrainId,
    onOwned: () => T | Promise<T>,
    onRemote: (owner: SiloAddress) => Promise<T>,
  ): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      const owner = this.ring().ownerOf(grainId);
      if (owner.equals(this.local)) {
        await this.onOwnedAccess(grainId);
        return onOwned();
      }
      try {
        return await onRemote(owner);
      } catch (err) {
        if (attempt >= MAX_STALE_RETRIES || !isStaleView(err)) throw err;
        this.refresh(); // advance our view, then recompute the owner and retry
      }
    }
  }
}

function isStaleView(err: unknown): boolean {
  return err instanceof RejectionError && err.kind === "staleView";
}
