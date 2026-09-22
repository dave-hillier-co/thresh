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
   * Resolve the owner of `grainId` and run the operation there. Owned-here reads
   * wait for any in-flight range recovery first; remote calls re-resolve and
   * retry (bounded) when the owner reports our view is stale.
   */
  private async route<T>(
    grainId: GrainId,
    onOwned: () => T | Promise<T>,
    onRemote: (owner: SiloAddress) => Promise<T>,
    record: (locality: DirectoryLocality) => void = () => undefined,
  ): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      const ring = this.ring();
      // An empty ring has no owner to resolve, and `ConsistentHashRing.ownerOf`
      // reports that as an invariant violation — a plain `Error`. Reached through
      // here it is not one: an empty ring means this silo's membership view has no
      // active silos at all (a static/dev view emptied by `setSilos([])`; the
      // Kubernetes watch keeps itself in its own view, so it never empties — see
      // issue #72). Refusing to route on it is a membership refusal the caller can
      // retry once its view advances, so reject with the kind that says so,
      // `staleView` — the kind the dispatcher's stale-rejection predicate and the
      // bounded remote retry below act on — rather than a plain `Error` that every
      // classifier reads as a programming fault (issue #70).
      if (ring.isEmpty) {
        throw new RejectionError("no active silos to resolve a directory owner", "staleView");
      }
      const owner = ring.ownerOf(grainId);
      if (owner.equals(this.local)) {
        record("local");
        await this.onOwnedAccess(grainId);
        return onOwned();
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
}

function isStaleView(err: unknown): boolean {
  return err instanceof RejectionError && err.kind === "staleView";
}
