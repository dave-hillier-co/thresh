import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { TimeProvider } from "@thresh/core/time-provider";
import {
  recordDirectoryLookup,
  recordDirectoryRegistration,
  type DirectoryLocality,
} from "@thresh/observability/directory-metrics";
import type { DirectoryPeer } from "@thresh/directory/directory-peer";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import type {
  DirectoryResult,
  GrainDirectoryPartition,
} from "@thresh/directory/grain-directory-partition";

/** Re-resolutions a request makes after owners say "refresh required", before it is refused. */
const DEFAULT_MAX_ATTEMPTS = 10;
/** The pause before the first retry; it grows by half each time, to at most `MAX_RETRY_DELAY_MS`. */
const INITIAL_RETRY_DELAY_MS = 10;
const MAX_RETRY_DELAY_MS = 500;

export interface DistributedGrainDirectoryOptions {
  local: SiloAddress;
  /** This silo's partition, which also holds the view this silo has applied. */
  partition: GrainDirectoryPartition;
  /** Carries operations to the partitions of other silos. */
  peer: DirectoryPeer;
  /**
   * Bring this silo's applied view up to at least `version` (refreshing membership): how a
   * caller told to refresh catches up before it re-resolves the owner.
   */
  refresh: (version: number) => Promise<void>;
  /** The clock the pause between retries runs on. */
  time: TimeProvider;
  maxAttempts?: number;
}

/**
 * The distributed directory as seen from one silo (Orleans `DistributedGrainDirectory`, the
 * client half). The view this silo has applied decides which silo owns a grain's entry; the
 * request carries that view's version to the owner, which catches up if it is behind, waits out
 * any range lock on the grain's hash, and answers "refresh required" if it does not own the hash.
 * The caller then refreshes to the owner's version, re-resolves and retries after a pause, a
 * bounded number of times.
 *
 * Requests are sent at a view at least as new as this silo's recovery version — the newest view
 * at which a member recovering a range has asked this silo for its activations — and an
 * operation during which the recovery version rose is sent again, so a registration can never
 * land at an owner the recovering member has already superseded (Orleans `InvokeAsync`).
 */
export class DistributedGrainDirectory implements GrainDirectory {
  private readonly local: SiloAddress;
  private readonly partition: GrainDirectoryPartition;
  private readonly peer: DirectoryPeer;
  private readonly refresh: (version: number) => Promise<void>;
  private readonly time: TimeProvider;
  private readonly maxAttempts: number;

  constructor(options: DistributedGrainDirectoryOptions) {
    this.local = options.local;
    this.partition = options.partition;
    this.peer = options.peer;
    this.refresh = options.refresh;
    this.time = options.time;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  }

  /** See `GrainDirectoryPartition.recoveryVersion`. */
  get recoveryVersion(): number {
    return this.partition.recoveryVersion;
  }

  async lookup(grainId: GrainId): Promise<GrainAddress | undefined> {
    return this.route(
      grainId,
      (version) => this.partition.lookup(version, grainId),
      (owner, version) => this.peer.lookup(owner, version, grainId),
      recordDirectoryLookup,
    );
  }

  async register(addr: GrainAddress, previous?: GrainAddress): Promise<GrainAddress> {
    return this.route(
      addr.grainId,
      (version) => this.partition.register(version, addr, previous),
      (owner, version) => this.peer.register(owner, version, addr, previous),
      recordDirectoryRegistration,
    );
  }

  async unregister(addr: GrainAddress): Promise<void> {
    await this.route(
      addr.grainId,
      (version) => this.partition.unregister(version, addr),
      (owner, version) => this.peer.unregister(owner, version, addr),
    );
  }

  /**
   * Nothing to do: each partition deletes the entries of a silo that has left when it applies
   * the view without it (Orleans `UnregisterSilos` on the distributed directory is a no-op too).
   */
  async unregisterSilo(_silo: SiloAddress): Promise<void> {}

  private async route<T>(
    grainId: GrainId,
    onLocal: (version: number) => Promise<DirectoryResult<T>>,
    onRemote: (owner: SiloAddress, version: number) => Promise<DirectoryResult<T>>,
    record: (locality: DirectoryLocality) => void = () => undefined,
  ): Promise<T> {
    const hash = grainId.getUniformHashCode();
    let delayMs = INITIAL_RETRY_DELAY_MS;
    for (let attempt = 1; ; attempt++) {
      const recoveryVersion = this.partition.recoveryVersion;
      if (this.partition.view.version < recoveryVersion) {
        await this.refresh(recoveryVersion);
      }
      const view = this.partition.view;
      const owner = view.ownerOf(hash);
      // An empty ring has no owner: this silo's view has no silo that may own a range. That is a
      // membership state the caller can retry once its view advances, so it is refused as a stale
      // view, the kind the dispatcher's stale-rejection handling acts on (issue #70).
      if (owner === undefined) {
        throw new RejectionError("no silo in the ring to resolve a directory owner", "staleView");
      }
      const isLocal = owner.equals(this.local);
      if (attempt === 1) record(isLocal ? "local" : "remote");
      const result = isLocal ? await onLocal(view.version) : await onRemote(owner, view.version);
      const reissue = this.partition.recoveryVersion !== recoveryVersion;
      if (!reissue && result.kind === "ok") return result.value;
      if (attempt >= this.maxAttempts) {
        throw new RejectionError(
          `directory owner of ${grainId.toString()} still moving after ${attempt} attempts`,
          "staleView",
        );
      }
      if (result.kind === "refresh" && result.version > this.partition.view.version) {
        await this.refresh(result.version);
      }
      await this.pause(delayMs);
      delayMs = Math.min(delayMs * 1.5, MAX_RETRY_DELAY_MS);
    }
  }

  private pause(ms: number): Promise<void> {
    return new Promise((resolve) => this.time.setTimer(resolve, ms));
  }
}
