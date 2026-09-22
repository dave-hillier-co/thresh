import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import {
  recordDirectoryLookup,
  recordDirectoryRegistration,
  type DirectoryLocality,
} from "@thresh/observability/directory-metrics";
import type { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import type { DirectoryPeer } from "@thresh/directory/directory-peer";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import type { LocalDirectoryPartition } from "@thresh/directory/local-directory-partition";

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
      recordDirectoryLookup,
    );
  }

  async register(addr: GrainAddress, previous?: GrainAddress): Promise<GrainAddress> {
    return this.route(
      addr.grainId,
      () => this.partition.register(addr, previous),
      (owner) => this.peer.register(owner, addr, previous),
      recordDirectoryRegistration,
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
   * Resolve the owner of `grainId` and run the operation there. Owned-here
   * operations wait for any in-flight range recovery first, then re-check that
   * the range is still ours before touching the partition; remote calls
   * re-resolve and retry (bounded) when the owner reports our view is stale.
   */
  private async route<T>(
    grainId: GrainId,
    onOwned: () => T | Promise<T>,
    onRemote: (owner: SiloAddress) => Promise<T>,
    record: (locality: DirectoryLocality) => void = () => undefined,
  ): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      const owner = this.ring().ownerOf(grainId);
      if (owner.equals(this.local)) {
        await this.onOwnedAccess(grainId);
        // That await yields — a microtask at the very least, with no recovery to
        // wait for — and `updateView` runs on exactly such a microtask (the
        // membership watch's own continuation). So the ring can move this range
        // elsewhere inside it: writing here regardless would leave an entry in a
        // partition the ring no longer assigns, which nothing re-drains until the
        // next view change, and the grain's true owner would then find no entry
        // and build a second activation. Re-resolve instead, the way `awaitView`
        // advances and then re-checks.
        if (this.ownsHere(grainId)) {
          record("local");
          return onOwned();
        }
        if (attempt >= MAX_STALE_RETRIES) {
          throw new RejectionError("directory ownership moved during the wait", "staleView");
        }
        this.refresh(); // advance our view, then recompute the owner and retry
        continue;
      }
      record("remote");
      try {
        return await onRemote(owner);
      } catch (err) {
        if (attempt >= MAX_STALE_RETRIES || !isStaleView(err)) throw err;
        this.refresh(); // advance our view, then recompute the owner and retry
      }
    }
  }

  /** Whether the range is this silo's under the CURRENT ring (an empty ring owns nothing). */
  private ownsHere(grainId: GrainId): boolean {
    const ring = this.ring();
    return !ring.isEmpty && ring.ownerOf(grainId).equals(this.local);
  }
}

function isStaleView(err: unknown): boolean {
  return err instanceof RejectionError && err.kind === "staleView";
}
