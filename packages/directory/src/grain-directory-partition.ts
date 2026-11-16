import { RejectionError } from "@thresh/core/errors";
import { grainAddressEquals, type GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import { type Logger, noopLogger } from "@thresh/core/logger";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { TimeProvider } from "@thresh/core/time-provider";
import {
  recordDirectoryRangeLockWait,
  recordDirectoryRecoveryCompleted,
  recordDirectoryRecoveryPatienceExpired,
  recordDirectoryRecoveryRetry,
  recordDirectoryRecoveryStarted,
  recordDirectorySnapshotTransfer,
} from "@thresh/observability/directory-metrics";
import { DirectoryView } from "@thresh/directory/directory-view";
import {
  RingRange,
  intersectRanges,
  rangesContain,
  rangesIntersect,
  subtractRanges,
} from "@thresh/directory/ring-range";

/**
 * A partition's answer to a request: the result, or "refresh required" with the partition's own
 * view version when it does not own the grain in that view (Orleans `DirectoryResult`).
 */
export type DirectoryResult<T> =
  | { readonly kind: "ok"; readonly value: T }
  | { readonly kind: "refresh"; readonly version: number };

/** A gained range's request to a previous owner: its snapshot of `ranges` for one view change. */
export interface SnapshotRequest {
  /** The view the requester moved from. */
  readonly previousVersion: number;
  /** The view the requester moved to. */
  readonly version: number;
  readonly ranges: readonly RingRange[];
}

/** A request for the activations a member hosts in `ranges`, made at the requester's `version`. */
export interface ActivationsRequest {
  readonly version: number;
  readonly ranges: readonly RingRange[];
}

/**
 * How a partition reaches the other silos' partitions and directory clients. The node implements
 * it over the transport (and answers its own silo locally); a test implements it in memory.
 */
export interface PartitionPeers {
  /**
   * Ask `owner` for the entries it gave up in `request.ranges` when it moved between exactly
   * `request.previousVersion` and `request.version`. `undefined` when it holds no snapshot for
   * that pair; rejects when it cannot be reached.
   */
  getSnapshot(owner: SiloAddress, request: SnapshotRequest): Promise<GrainAddress[] | undefined>;
  /** Tell `owner` its snapshot for the pair has been applied, so it may drop it. Best effort. */
  acknowledgeSnapshot(
    owner: SiloAddress,
    pair: { readonly previousVersion: number; readonly version: number },
  ): Promise<void>;
  /**
   * Ask `member` for the registered activations it hosts in the ranges, as a recovery
   * (`DistributedGrainDirectory.RecoverRegisteredActivations`): it drops the requester from its
   * snapshots and raises its recovery version. A member with no process at its address
   * (connection refused) hosts nothing and resolves `[]`; one that does not answer rejects.
   */
  recoverActivations(member: SiloAddress, request: ActivationsRequest): Promise<GrainAddress[]>;
  /** The same answer for an integrity check, with no recovery side effects. */
  registeredActivations(member: SiloAddress, request: ActivationsRequest): Promise<GrainAddress[]>;
}

/** This silo's activations, as recovery and the integrity check need them. */
export interface LocalActivations {
  /**
   * The registered, live, non-deactivating activations this silo hosts whose grains hash into
   * `ranges`, at the addresses their registrations returned. Stateless workers and local system
   * activations are never registered, so never included.
   */
  registeredIn(ranges: readonly RingRange[]): GrainAddress[];
}

export interface GrainDirectoryPartitionOptions {
  local: SiloAddress;
  peers: PartitionPeers;
  activations: LocalActivations;
  /**
   * Bring this silo's applied view up to at least `version` (Orleans `RefreshViewAsync`): the
   * node refreshes membership and applies what it brings, which reaches `applyView`. Rejects if
   * the version is not reached in time.
   */
  refreshView: (version: number) => Promise<void>;
  time: TimeProvider;
  logger?: Logger;
  /**
   * How long a recovery may wait for members that do not answer before the range is unlocked
   * without them (`SiloConfig.directoryRecoveryPatience`). Unset, a recovery waits for as long
   * as those members are listed: the directory fails closed.
   */
  recoveryPatienceMs?: number;
  /** First delay before a recovery request to a member that did not answer is sent again. */
  retryInitialMs?: number;
  /** The delay between such retries grows to at most this. */
  retryMaxMs?: number;
}

/** A range locked across a view change: requests for a hash in it at or above its version wait. */
interface RangeLock {
  readonly ranges: readonly RingRange[];
  readonly version: number;
  readonly done: Promise<void>;
  readonly release: () => void;
  readonly fail: (error: unknown) => void;
}

/** Entries of ranges this silo gave up, held for the silos that took them. */
interface Snapshot {
  readonly previousVersion: number;
  readonly version: number;
  readonly entries: readonly GrainAddress[];
  partners: SiloAddress[];
}

/** One integrity violation `checkIntegrity` found. */
export interface IntegrityViolation {
  readonly silo: SiloAddress;
  readonly kind: "outside-range" | "missing" | "mismatched" | "unanswered";
  readonly grainId?: GrainId;
  readonly detail: string;
}

const DEFAULT_RETRY_INITIAL_MS = 100;
const DEFAULT_RETRY_MAX_MS = 2_000;
/** How many times the integrity check asks a member before reporting it unanswered. */
const INTEGRITY_ATTEMPTS = 4;

/**
 * One silo's partition of the distributed grain directory: a port of Orleans
 * `GrainDirectoryPartition`, independent of the transport.
 *
 * It holds the entries for the hashes this silo owns in the view it has applied, each stamped
 * with that view's version when registered. A view change is applied synchronously up to and
 * including taking range locks (`applyView`), so no request can run between the ring changing
 * and the range being locked. A lost range is moved into a snapshot keyed by the pair of views
 * the change moved between, for the new owners to take; a gained range stays locked until its
 * entries have been transferred from every previous owner's snapshot for exactly that pair, or
 * rebuilt from the activations every member hosts (recovery). Requests carry the caller's view
 * version, wait for this partition to reach it and for any lock on the grain's hash, and are told
 * to refresh when this partition does not own the hash. Nothing is reported missing because it is
 * in transit: callers wait.
 */
export class GrainDirectoryPartition {
  private readonly local: SiloAddress;
  private readonly peers: PartitionPeers;
  private readonly activations: LocalActivations;
  private readonly time: TimeProvider;
  private readonly logger: Logger;
  private readonly options: GrainDirectoryPartitionOptions;
  private readonly entries = new Map<string, GrainAddress>();
  private locks: RangeLock[] = [];
  private snapshots: Snapshot[] = [];
  private current: DirectoryView = DirectoryView.initial;
  private recoveryVersionValue = -1;
  private stopped = false;
  private readonly viewChanges = new Set<Promise<void>>();
  /** Releases of lost ranges still moving their entries into a snapshot. */
  private releasing = 0;
  /** Run whenever a snapshot is pushed or dropped, a release finishes, or the partition stops. */
  private readonly handoverListeners = new Set<() => void>();
  private readonly counts = {
    transfers: 0,
    recoveries: 0,
    transferred: 0,
    recovered: 0,
    patienceExpired: 0,
  };

  constructor(options: GrainDirectoryPartitionOptions) {
    this.options = options;
    this.local = options.local;
    this.peers = options.peers;
    this.activations = options.activations;
    this.time = options.time;
    this.logger = options.logger ?? noopLogger;
  }

  /** The view this partition has applied. */
  get view(): DirectoryView {
    return this.current;
  }

  /**
   * The highest view version at which a member has asked this silo for its activations
   * (Orleans `DistributedGrainDirectory._recoveryMembershipVersion`). A registration completed
   * below it may have been lost with an owner that crashed, so it is confirmed again, and this
   * silo's directory requests are sent at a view at least this new.
   */
  get recoveryVersion(): number {
    return this.recoveryVersionValue;
  }

  /** Entries held (introspection). */
  get size(): number {
    return this.entries.size;
  }

  /** Snapshots held for transfer (introspection). */
  get snapshotCount(): number {
    return this.snapshots.length;
  }

  /** Ranges locked now (introspection). */
  get lockedRanges(): readonly RingRange[] {
    return this.locks.flatMap((l) => l.ranges);
  }

  /**
   * Ranges acquired by snapshot transfer and by recovery, the entries each brought in, and
   * recoveries abandoned when the patience elapsed (introspection and metrics).
   */
  get stats(): {
    transfers: number;
    recoveries: number;
    transferred: number;
    recovered: number;
    patienceExpired: number;
  } {
    return { ...this.counts };
  }

  /** The entry held for `grainId`, whatever the view (introspection; requests go through `lookup`). */
  entryFor(grainId: GrainId): GrainAddress | undefined {
    return this.entries.get(grainId.toString());
  }

  // ── View changes ───────────────────────────────────────────────────────────

  /**
   * Apply a view (Orleans `ProcessMembershipUpdate`). A view no newer than the applied one is
   * ignored. Everything up to and including taking the locks happens before this returns:
   *
   * 1. Entries hosted on silos that are not members of `next` are deleted, and those silos
   *    leave every snapshot's partners.
   * 2. Ranges lost are locked, then (once earlier locks on them are released) their entries move
   *    into a snapshot for `(previous version, next version)` whose partners are their new
   *    owners, and the lock is released.
   * 3. Ranges gained are locked until their entries have been transferred from every previous
   *    owner's snapshot for exactly that pair, or recovered from every member.
   */
  applyView(next: DirectoryView): void {
    if (this.stopped || next.version <= this.current.version) return;
    const previous = this.current;
    this.current = next;

    for (const [key, entry] of this.entries) {
      if (!next.isMember(entry.silo)) this.entries.delete(key);
    }
    this.dropPartners(
      (silo) => !next.isMember(silo),
      () => true,
    );

    const before = previous.rangesOf(this.local);
    const after = next.rangesOf(this.local);
    const lost = subtractRanges(before, after);
    const gained = subtractRanges(after, before);
    if (lost.length > 0) {
      const lock = this.lock(lost, next.version);
      this.releasing++;
      this.track(this.release(previous, next, lost, lock));
    }
    if (gained.length > 0) {
      const lock = this.lock(gained, next.version);
      this.track(this.acquire(previous, next, gained, lock));
    }
    for (const waiter of this.viewWaiters.splice(0)) {
      if (next.version >= waiter.version) waiter.resolve();
      else this.viewWaiters.push(waiter);
    }
  }

  private readonly viewWaiters: Array<{ version: number; resolve: () => void }> = [];

  /** Resolves once every view change applied so far has finished transferring or recovering. */
  async settled(): Promise<void> {
    while (this.viewChanges.size > 0) await Promise.allSettled([...this.viewChanges]);
  }

  /** Stop: no further view is applied, nothing is retried, and every waiting request is refused. */
  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    const refusal = new RejectionError(
      `the directory partition on ${this.local.toString()} has stopped`,
      "siloDraining",
    );
    for (const lock of this.locks) lock.fail(refusal);
    this.notifyHandover();
  }

  /**
   * Resolves once every range this partition gave up has been handed over to a ready partner —
   * no release is still moving entries into a snapshot, and no snapshot held has a partner that is
   * `active` in the view this partition has applied — or once `timeoutMs` has elapsed on this
   * partition's clock, or once it stops, whichever comes first. It resolves `true` when the
   * handover completed and `false` when the bound elapsed first.
   *
   * A snapshot is dropped when every partner has acknowledged it, left the view, or started a
   * recovery at or above its version. A snapshot whose remaining partners are all not ready does
   * not hold this up: such a partner owns a range only by the cold-start fallback (no member is
   * active), and nothing says its process is running, so a dead pod that is still listed would
   * otherwise hold the leaver for the whole bound. That snapshot is still held, and still served
   * if one of its partners comes for it; and a partner that turns active while this waits holds
   * it up again. A partition that holds nothing a ready partner is owed resolves at once, and no
   * timer is left armed either way. A leaving silo waits on this before it deactivates
   * (`ClusterNode.stop`).
   */
  handedOver(timeoutMs: number): Promise<boolean> {
    if (this.isHandedOver()) return Promise.resolve(true);
    return new Promise<boolean>((resolve) => {
      const done = (completed: boolean): void => {
        this.handoverListeners.delete(check);
        this.time.clearTimer(timer);
        resolve(completed);
      };
      const check = (): void => {
        if (this.isHandedOver()) done(true);
      };
      this.handoverListeners.add(check);
      const timer = this.time.setTimer(() => done(false), timeoutMs);
    });
  }

  /**
   * The partners a leaver still waits for (introspection and logging): every `active` partner of
   * a snapshot held. Empty when `handedOver` would not wait on any snapshot.
   */
  get awaitedPartners(): readonly SiloAddress[] {
    const out: SiloAddress[] = [];
    for (const s of this.snapshots) {
      for (const p of s.partners) {
        if (this.current.isActive(p) && !out.some((o) => o.equals(p))) out.push(p);
      }
    }
    return out;
  }

  private isHandedOver(): boolean {
    if (this.stopped) return true;
    return this.releasing === 0 && this.awaitedPartners.length === 0;
  }

  private notifyHandover(): void {
    for (const listener of [...this.handoverListeners]) listener();
  }

  private track(change: Promise<void>): void {
    this.viewChanges.add(change);
    void change.finally(() => this.viewChanges.delete(change));
  }

  private lock(ranges: readonly RingRange[], version: number): RangeLock {
    let release!: () => void;
    let fail!: (error: unknown) => void;
    const done = new Promise<void>((resolve, reject) => {
      release = resolve;
      fail = reject;
    });
    // A waiter observes the rejection; an unobserved one must not surface as unhandled.
    done.catch(() => undefined);
    const lock: RangeLock = { ranges, version, done, release, fail };
    this.locks.push(lock);
    return lock;
  }

  private unlock(lock: RangeLock): void {
    this.locks = this.locks.filter((l) => l !== lock);
    lock.release();
  }

  /** Orleans `ReleaseRangeAsync`. */
  private async release(
    previous: DirectoryView,
    next: DirectoryView,
    lost: readonly RingRange[],
    lock: RangeLock,
  ): Promise<void> {
    try {
      await this.waitForRanges(lost, previous.version);
      const moved: GrainAddress[] = [];
      for (const [key, entry] of this.entries) {
        if (rangesContain(lost, entry.grainId.getUniformHashCode())) {
          moved.push(entry);
          this.entries.delete(key);
        }
      }
      const partners = next.ringSilos.filter(
        (silo) => !silo.equals(this.local) && rangesIntersect(next.rangesOf(silo), lost),
      );
      if (partners.length > 0) {
        this.snapshots.push({
          previousVersion: previous.version,
          version: next.version,
          entries: moved,
          partners,
        });
      }
    } catch (error) {
      if (!this.stopped) {
        this.logger.error("directory partition failed to release ranges", {
          silo: this.local.toString(),
          version: next.version,
          error: errorText(error),
        });
      }
    } finally {
      if (!this.stopped) this.unlock(lock);
      this.releasing--;
      this.notifyHandover();
    }
  }

  /**
   * Orleans `AcquireRangeAsync`: take the entries of `gained` from its previous owners, or
   * recover them, and only then unlock. An unexpected failure on the way is logged and the
   * acquisition retried with recovery, the range still locked; nothing unlocks it empty except
   * `recoveryPatienceMs`, when configured.
   */
  private async acquire(
    previous: DirectoryView,
    next: DirectoryView,
    gained: readonly RingRange[],
    lock: RangeLock,
  ): Promise<void> {
    const patience = this.startPatience(next, gained);
    try {
      let transferFirst = true;
      for (let attempt = 0; !this.stopped && !patience.expired; attempt++) {
        try {
          const transferred = transferFirst && (await this.transfer(previous, next, gained));
          if (!transferred) {
            await this.waitForRanges(gained, previous.version);
            await this.recover(next, gained, patience);
          }
          return;
        } catch (error) {
          transferFirst = false;
          if (this.stopped || patience.expired) return;
          this.logger.error("directory range acquisition failed; retrying with recovery", {
            silo: this.local.toString(),
            version: next.version,
            ranges: gained.map(String),
            error: errorText(error),
          });
          await this.delay(this.retryDelay(attempt));
        }
      }
    } finally {
      patience.clear();
      if (!this.stopped) this.unlock(lock);
    }
  }

  /**
   * Take `gained` from its previous owners' snapshots for exactly `(previous.version,
   * next.version)`. `false` when that is not possible — the previous view did not cover the
   * range (a first view, or an empty ring), a previous owner is no longer a member, or a previous
   * owner could not be reached or holds no snapshot for that pair (it skipped or coalesced a
   * view) — so the caller recovers instead.
   */
  private async transfer(
    previous: DirectoryView,
    next: DirectoryView,
    gained: readonly RingRange[],
  ): Promise<boolean> {
    const sources = previous.ringSilos
      .map((silo) => ({ silo, ranges: intersectRanges(previous.rangesOf(silo), gained) }))
      .filter((s) => s.ranges.length > 0);
    const covered = sources.flatMap((s) => s.ranges);
    if (subtractRanges(gained, covered).length > 0) return false;
    if (sources.some((s) => !next.isMember(s.silo))) return false;
    const pair = { previousVersion: previous.version, version: next.version };
    const snapshots = await Promise.all(
      sources.map(async (source) => {
        try {
          return await this.peers.getSnapshot(source.silo, { ...pair, ranges: source.ranges });
        } catch (error) {
          this.logger.warn("directory snapshot transfer failed; the range will be recovered", {
            silo: this.local.toString(),
            from: source.silo.toString(),
            ...pair,
            error: errorText(error),
          });
          return undefined;
        }
      }),
    );
    // A previous owner that holds no snapshot for this pair moved between other views.
    if (snapshots.some((s) => s === undefined)) return false;
    await this.waitForRanges(gained, previous.version);
    let count = 0;
    for (const snapshot of snapshots) {
      for (const entry of snapshot!) {
        if (!rangesContain(gained, entry.grainId.getUniformHashCode())) continue;
        if (!this.current.isMember(entry.silo)) continue;
        this.entries.set(entry.grainId.toString(), entry);
        count++;
      }
    }
    this.counts.transferred += count;
    this.counts.transfers++;
    for (const source of sources) {
      void this.peers.acknowledgeSnapshot(source.silo, pair).catch(() => undefined);
    }
    recordDirectorySnapshotTransfer();
    return true;
  }

  /**
   * Orleans `RecoverPartitionRange`: rebuild `gained` from the registered activations every
   * member hosts in it. A member that does not answer is asked again, with backoff, for as long
   * as it is a member; one that leaves the view stops being asked.
   */
  private async recover(
    next: DirectoryView,
    gained: readonly RingRange[],
    patience: Patience,
  ): Promise<void> {
    recordDirectoryRecoveryStarted();
    this.counts.recoveries++;
    const request: ActivationsRequest = { version: next.version, ranges: gained };
    const answers = await Promise.all(
      next.members.map((member) => this.recoverFrom(member, request, patience)),
    );
    // The range is rebuilt from the hosts' answers: whatever a partial transfer left is replaced.
    for (const [key, entry] of this.entries) {
      if (rangesContain(gained, entry.grainId.getUniformHashCode())) this.entries.delete(key);
    }
    let count = 0;
    for (const answer of answers) {
      for (const entry of answer) {
        if (!rangesContain(gained, entry.grainId.getUniformHashCode())) continue;
        if (!this.current.isMember(entry.silo)) continue;
        this.entries.set(entry.grainId.toString(), entry);
        count++;
      }
    }
    this.counts.recovered += count;
    if (!patience.expired) recordDirectoryRecoveryCompleted();
  }

  private async recoverFrom(
    member: SiloAddress,
    request: ActivationsRequest,
    patience: Patience,
  ): Promise<GrainAddress[]> {
    for (let attempt = 0; ; attempt++) {
      if (this.stopped || patience.expired || !this.current.isMember(member)) return [];
      try {
        return await patience.race(this.peers.recoverActivations(member, request), []);
      } catch (error) {
        if (this.stopped || patience.expired || !this.current.isMember(member)) return [];
        recordDirectoryRecoveryRetry();
        this.logger.warn("directory recovery: a member did not answer; asking again", {
          silo: this.local.toString(),
          member: member.toString(),
          version: request.version,
          attempt: attempt + 1,
          error: errorText(error),
        });
        await patience.race(this.delay(this.retryDelay(attempt)), undefined);
      }
    }
  }

  private startPatience(next: DirectoryView, gained: readonly RingRange[]): Patience {
    const patience = new Patience();
    const ms = this.options.recoveryPatienceMs;
    if (ms === undefined) return patience;
    const handle = this.time.setTimer(() => {
      patience.expire();
      this.counts.patienceExpired++;
      recordDirectoryRecoveryPatienceExpired();
      this.logger.error(
        "directory recovery patience elapsed: unlocking ranges without every member's answer",
        {
          silo: this.local.toString(),
          version: next.version,
          ranges: gained.map(String),
          patienceMs: ms,
        },
      );
    }, ms);
    patience.onClear(() => this.time.clearTimer(handle));
    return patience;
  }

  private retryDelay(attempt: number): number {
    const initial = this.options.retryInitialMs ?? DEFAULT_RETRY_INITIAL_MS;
    const max = this.options.retryMaxMs ?? DEFAULT_RETRY_MAX_MS;
    return Math.min(initial * 2 ** attempt, max);
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => this.time.setTimer(resolve, ms));
  }

  /**
   * Wait until this partition has applied `version` and no lock at or below `version` holds any
   * of `ranges` (Orleans `WaitForRange`).
   */
  private async waitForRanges(ranges: readonly RingRange[], version: number): Promise<void> {
    const startedAt = this.time.now();
    let waited = false;
    if (this.current.version < version) {
      waited = true;
      await this.reachView(version);
    }
    for (;;) {
      const lock = this.locks.find(
        (l) => l.version <= version && rangesIntersect(l.ranges, ranges),
      );
      if (lock === undefined) break;
      waited = true;
      await lock.done;
    }
    if (waited) recordDirectoryRangeLockWait(this.time.now() - startedAt);
  }

  private async reachView(version: number): Promise<void> {
    if (this.current.version >= version) return;
    const reached = new Promise<void>((resolve) => this.viewWaiters.push({ version, resolve }));
    await Promise.race([reached, this.options.refreshView(version).then(() => reached)]);
  }

  /**
   * Orleans `WaitForOwnershipViewAsync`: wait for the view a request needs — at least the
   * caller's, and the latest this partition holds — and for every lock on the hash at or below
   * it, until no view change lands during the wait.
   */
  private async ownershipView(hash: number, version: number): Promise<DirectoryView> {
    const point = [RingRange.point(hash)];
    for (;;) {
      const view = this.current;
      await this.waitForRanges(point, Math.max(view.version, version));
      if (view === this.current) return view;
    }
  }

  // ── Requests ──────────────────────────────────────────────────────────────

  async lookup(
    version: number,
    grainId: GrainId,
  ): Promise<DirectoryResult<GrainAddress | undefined>> {
    const hash = grainId.getUniformHashCode();
    const view = await this.ownershipView(hash, version);
    if (!view.owns(this.local, hash)) return { kind: "refresh", version: view.version };
    const entry = this.entries.get(grainId.toString());
    // Orleans `LookupCore`/`IsSiloDead`: an entry whose host is no longer a member is a miss.
    return {
      kind: "ok",
      value: entry !== undefined && view.isMember(entry.silo) ? entry : undefined,
    };
  }

  /**
   * Compare-and-swap registration (Orleans `RegisterCore`): `addr` wins when no entry exists,
   * when the existing entry's host is no longer a member, or when the caller names the exact
   * entry it replaces (`previous`). The winner is stamped with this partition's view version.
   * Otherwise the existing entry is returned and the caller defers to it.
   */
  async register(
    version: number,
    addr: GrainAddress,
    previous?: GrainAddress,
  ): Promise<DirectoryResult<GrainAddress>> {
    const hash = addr.grainId.getUniformHashCode();
    const view = await this.ownershipView(hash, version);
    if (!view.owns(this.local, hash)) return { kind: "refresh", version: view.version };
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (
      existing === undefined ||
      !view.isMember(existing.silo) ||
      (previous !== undefined && grainAddressEquals(existing, previous))
    ) {
      const stamped: GrainAddress = {
        grainId: addr.grainId,
        silo: addr.silo,
        activationId: addr.activationId,
        version: view.version,
      };
      this.entries.set(key, stamped);
      return { kind: "ok", value: stamped };
    }
    return { kind: "ok", value: existing };
  }

  /** Remove `addr`'s entry if it is the one held, or if the one held points at a non-member. */
  async unregister(version: number, addr: GrainAddress): Promise<DirectoryResult<undefined>> {
    const hash = addr.grainId.getUniformHashCode();
    const view = await this.ownershipView(hash, version);
    if (!view.owns(this.local, hash)) return { kind: "refresh", version: view.version };
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (
      existing !== undefined &&
      (grainAddressEquals(existing, addr) || !view.isMember(existing.silo))
    ) {
      this.entries.delete(key);
    }
    return { kind: "ok", value: undefined };
  }

  // ── Serving peers ──────────────────────────────────────────────────────────

  /**
   * A new owner's request for the entries this silo gave up in `request.ranges` between exactly
   * `request.previousVersion` and `request.version` (Orleans `GetSnapshotAsync`). Waits until
   * this silo has applied `request.version` and its locks on the ranges at or below it are
   * released. `undefined` when no snapshot for that pair is held: the requester recovers.
   */
  async serveSnapshot(request: SnapshotRequest): Promise<GrainAddress[] | undefined> {
    await this.waitForRanges(request.ranges, request.version);
    const snapshot = this.snapshots.find(
      (s) => s.previousVersion === request.previousVersion && s.version === request.version,
    );
    // This silo did not move between that pair of views (it skipped or coalesced one).
    if (snapshot === undefined) return undefined;
    return snapshot.entries.filter((e) =>
      rangesContain(request.ranges, e.grainId.getUniformHashCode()),
    );
  }

  /** `from` applied this silo's snapshot for the pair: it is no longer a partner of it. */
  acknowledgeSnapshot(
    from: SiloAddress,
    pair: { readonly previousVersion: number; readonly version: number },
  ): void {
    this.dropPartners(
      (silo) => silo.equals(from),
      (s) => s.previousVersion === pair.previousVersion && s.version === pair.version,
    );
  }

  /**
   * A member recovering ranges at `request.version` asks for this silo's activations in them
   * (Orleans `RecoverRegisteredActivations`). The requester stops being a partner of every
   * snapshot at or below that version — it will not come for them — once this silo's locks on
   * the ranges are released; this silo's recovery version rises to the request's; and the
   * answer is its registered activations in the ranges.
   */
  answerRecovery(requester: SiloAddress, request: ActivationsRequest): GrainAddress[] {
    void this.waitForRanges(request.ranges, request.version)
      .catch(() => undefined)
      .then(() =>
        this.dropPartners(
          (silo) => silo.equals(requester),
          (s) => s.version <= request.version,
        ),
      );
    if (request.version > this.recoveryVersionValue) this.recoveryVersionValue = request.version;
    return this.activations.registeredIn(request.ranges);
  }

  /** The same answer for an integrity check: no snapshot dropped, no recovery version raised. */
  answerRegistered(request: ActivationsRequest): GrainAddress[] {
    return this.activations.registeredIn(request.ranges);
  }

  private dropPartners(
    partner: (silo: SiloAddress) => boolean,
    snapshot: (s: Snapshot) => boolean,
  ): void {
    for (const s of this.snapshots) {
      if (snapshot(s)) s.partners = s.partners.filter((p) => !partner(p));
    }
    this.snapshots = this.snapshots.filter((s) => s.partners.length > 0);
    this.notifyHandover();
  }

  // ── Integrity ──────────────────────────────────────────────────────────────

  /**
   * A test hook (Orleans `IGrainDirectoryTestHooks.CheckIntegrityAsync`): with the whole range
   * locked at the applied view, check that no entry lies outside this partition's ranges, and
   * that every registered live activation any member reports in them has exactly its own entry
   * here. Returns the violations found; none means the partition is sound.
   */
  async checkIntegrity(): Promise<IntegrityViolation[]> {
    const view = this.current;
    await this.waitForRanges([RingRange.full], view.version);
    const lock = this.lock([RingRange.full], view.version);
    const violations: IntegrityViolation[] = [];
    try {
      const ranges = view.rangesOf(this.local);
      for (const entry of this.entries.values()) {
        if (!rangesContain(ranges, entry.grainId.getUniformHashCode())) {
          violations.push({
            silo: this.local,
            kind: "outside-range",
            grainId: entry.grainId,
            detail: `entry ${describe(entry)} is outside the ranges ${this.local.podName} owns at view ${view.version}`,
          });
        }
      }
      if (ranges.length === 0) return violations;
      const request: ActivationsRequest = { version: view.version, ranges };
      const answers = await Promise.all(
        view.members.map(async (member) => {
          // Asked again, as a recovery asks again, a few times before it counts as unanswered.
          for (let attempt = 0; ; attempt++) {
            try {
              const activations = await this.peers.registeredActivations(member, request);
              return { member, activations };
            } catch (error) {
              // A member that has left the view hosts nothing that needs an entry.
              if (!this.current.isMember(member)) return { member, activations: [] };
              if (attempt < INTEGRITY_ATTEMPTS - 1) {
                await this.delay(this.retryDelay(attempt));
                continue;
              }
              violations.push({
                silo: this.local,
                kind: "unanswered",
                detail: `${member.podName} did not report its activations: ${errorText(error)}`,
              });
              return { member, activations: [] as GrainAddress[] };
            }
          }
        }),
      );
      for (const { activations } of answers) {
        for (const activation of activations) {
          if (!view.owns(this.local, activation.grainId.getUniformHashCode())) continue;
          const entry = this.entries.get(activation.grainId.toString());
          if (entry === undefined) {
            violations.push({
              silo: this.local,
              kind: "missing",
              grainId: activation.grainId,
              detail: `activation ${describe(activation)} has no entry at its owner ${this.local.podName}`,
            });
          } else if (!grainAddressEquals(entry, activation)) {
            violations.push({
              silo: this.local,
              kind: "mismatched",
              grainId: activation.grainId,
              detail: `activation ${describe(activation)} does not match the entry ${describe(entry)} at ${this.local.podName}`,
            });
          }
        }
      }
      return violations;
    } finally {
      this.unlock(lock);
    }
  }
}

/** A recovery's optional deadline: once expired, every wait in it stops. */
class Patience {
  expired = false;
  private readonly expiredListeners: Array<() => void> = [];
  private readonly clearListeners: Array<() => void> = [];

  expire(): void {
    if (this.expired) return;
    this.expired = true;
    for (const listener of this.expiredListeners.splice(0)) listener();
  }

  onClear(listener: () => void): void {
    this.clearListeners.push(listener);
  }

  clear(): void {
    for (const listener of this.clearListeners.splice(0)) listener();
  }

  /** `work`, or `fallback` as soon as the patience expires, whichever comes first. */
  race<T>(work: Promise<T>, fallback: T): Promise<T> {
    if (this.expired) return Promise.resolve(fallback);
    return new Promise<T>((resolve, reject) => {
      this.expiredListeners.push(() => resolve(fallback));
      work.then(resolve, reject);
    });
  }
}

function describe(addr: GrainAddress): string {
  return `${addr.grainId.toString()}@${addr.silo.podName}/${addr.activationId}`;
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
