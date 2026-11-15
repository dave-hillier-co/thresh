import { RejectionError } from "@thresh/core/errors";
import {
  MembershipFeed,
  type LocalStanding,
  type MembershipService,
  type MembershipSnapshot,
  type SiloMember,
} from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import {
  metadataFromSlices,
  siloMembersFromSlices,
  type EndpointSlice,
} from "@thresh/clustering-k8s/endpoint-slice";

/** How long `refresh` waits for the watch to deliver a version before it re-lists. */
const DEFAULT_REFRESH_WATCH_GRACE_MS = 100;

/**
 * How long `refresh` may take in all before it gives up. The default is the runtime's default
 * call timeout (30s): a refresh is awaited by a call (`ClusterNode.awaitView`) that is itself
 * bounded by the call timeout, so a refresh outliving it only holds a pending entry for a caller
 * that has already failed. `SiloBuilder.useKubernetesMembership` passes the silo's configured
 * call timeout instead when none is set here.
 */
const DEFAULT_REFRESH_TIMEOUT_MS = 30_000;

/**
 * Source of EndpointSlice updates. The production implementation wraps the
 * Kubernetes API watch; tests push slices directly. `subscribe` returns an
 * unsubscribe function.
 *
 * Every update carries its `revision`: the highest EndpointSlice `resourceVersion` the watch
 * has observed (see `WatchedEndpoints`), which becomes the view version. It is mandatory —
 * a view without one could not be compared with a peer's — and `KubernetesMembership` refuses
 * an update that lacks it.
 */
export interface EndpointWatch {
  subscribe(onSlices: (slices: EndpointSlice[], revision: number) => void): () => void;
  /**
   * List the slices again now, delivering the result to subscribers as any update is, and
   * resolve with the list's own `resourceVersion` as a revision (`undefined` when the API did
   * not report one). Optional: a watch without it can only be waited on. `refresh` uses it.
   */
  relist?(): Promise<number | undefined>;
}

export interface KubernetesMembershipOptions {
  /** Named service port to use for the silo endpoint (defaults to the first). */
  portName?: string;
  /**
   * Pod labels under this prefix populate `SiloMember.metadata`, with the prefix
   * stripped from the key — e.g. `thresh.io/role=worker` with prefix
   * `"thresh.io/"` surfaces as `{ role: "worker" }`, the same shape
   * `useStaticMembership`'s metadata resolver produces. Requires a watch source
   * that resolves pod labels onto `EndpointSliceEndpoint.metadata` (see
   * `createKubernetesClientSource`'s `fetchPodLabels` option); omit to leave
   * `SiloMember.metadata` unset.
   */
  metadataLabelPrefix?: string;
  /**
   * How long `refresh` waits for the watch to deliver the requested version before it
   * re-lists (defaults to 100ms). A peer that is ahead usually got there through a write this
   * silo's watch is about to deliver; the re-list covers the case where it never will (a
   * deletion missed while the watch was down).
   */
  refreshWatchGraceMs?: number;
  /**
   * The longest a `refresh` may take in all (defaults to 30s, the default call timeout; the
   * silo builder supplies its configured call timeout). When it elapses without the view
   * reaching the requested version the refresh rejects with the retriable `staleView` rejection,
   * and nothing of it is left pending.
   */
  refreshTimeoutMs?: number;
  /** The clock for the refresh grace and bound (defaults to the system clock). */
  time?: TimeProvider;
}

/**
 * Membership derived from Kubernetes: every silo watches the same EndpointSlices
 * for the headless service, so views converge without a gossip protocol. Each
 * reconciliation produces a new versioned snapshot (docs/05).
 *
 * **The view is a pure function of the slices.** Every listed endpoint — this silo's own
 * included — takes its status from its conditions (`siloStatusOf`): `ready` is `active`, not
 * ready and `terminating` is `draining`, not ready otherwise is `joining`, and an endpoint that
 * is not listed is not a member. There is no special case for the local silo: before its
 * endpoint is listed it is simply not in its own view, and two silos that have observed the same
 * slices hold the same view, so "same version, same ring" holds across the cluster
 * (`ringSilos`). This silo's own address must therefore be the one its endpoint advertises
 * (pod name, pod uid and `ip:port`).
 *
 * **The version is the observed EndpointSlice revision** (issue #72): the highest
 * `resourceVersion` the watch has seen across the listed slices and the watch events (see
 * `WatchedEndpoints`), which every update must carry. Two silos that have observed the same
 * writes to the service's slices hold the same version, and a silo whose watch is behind holds
 * a lower one — the cluster-wide, ordered view identity the directory's `staleView` guard and
 * `ClusterNode.awaitView` compare across silos. A re-list that brings nothing new publishes
 * nothing. `refresh` catches a silo up on demand when a peer shows it is behind.
 *
 * **Where this silo stands** (`localStanding`): `unlisted` until a view lists its endpoint,
 * `listed` while one does, and `removed` once a view omits it after it was listed. A removal is
 * final for this incarnation: its peers have already dropped its directory entries, so the host
 * fences it (fails liveness, refuses calls, abandons its activations) and Kubernetes restarts
 * the pod.
 */
export class KubernetesMembership implements MembershipService {
  private readonly feed = new MembershipFeed({ version: 0, silos: [] });
  private readonly unsubscribe: () => void;
  private readonly refreshWatchGraceMs: number;
  private readonly refreshTimeoutMs: number;
  private readonly time: TimeProvider;
  private stopped = false;
  /** Waits for a version, woken when a published view reaches it. */
  private readonly viewWaiters = new Set<{ version: number; wake: () => void }>();
  /** Every refresh in flight, so `stop` can settle them. */
  private readonly refreshes = new Set<AbortController>();
  private published = false;
  private standing: LocalStanding = "unlisted";
  /** The refresh in flight, shared by every caller asking for a version it covers. */
  private refreshing: { version: number; done: Promise<void> } | undefined;

  constructor(
    private readonly local: SiloAddress,
    private readonly watch: EndpointWatch,
    private readonly options: KubernetesMembershipOptions = {},
  ) {
    this.refreshWatchGraceMs = options.refreshWatchGraceMs ?? DEFAULT_REFRESH_WATCH_GRACE_MS;
    this.refreshTimeoutMs = options.refreshTimeoutMs ?? DEFAULT_REFRESH_TIMEOUT_MS;
    this.time = options.time ?? systemTimeProvider;
    this.unsubscribe = watch.subscribe((slices, revision) => this.onSlices(slices, revision));
  }

  current(): MembershipSnapshot {
    return this.feed.current();
  }

  localSilo(): SiloAddress {
    return this.local;
  }

  localStanding(): LocalStanding {
    return this.standing;
  }

  updates(): AsyncIterableIterator<MembershipSnapshot> {
    return this.feed.updates();
  }

  /**
   * Resolve once this silo holds a view at or above `version` (Orleans
   * `ClusterMembershipService.Refresh`). It waits briefly for the watch to deliver it, then
   * re-lists: a list whose own `resourceVersion` is at least `version` proves that the slices it
   * returned reflect every write up to `version`, so the view taken from them is at least that
   * new and is numbered `version` — whatever it contains, it is what the list says. That closes
   * the case of a silo that listed after a slice deletion it never saw, which would otherwise sit
   * behind its peers until the next unrelated write. A list that proves less leaves the wait to
   * the watch; the caller bounds it.
   */
  refresh(version: number): Promise<void> {
    if (this.current().version >= version) return Promise.resolve();
    if (this.stopped) return Promise.reject(this.refreshFailure(version, "the service has stopped"));
    const inFlight = this.refreshing;
    if (inFlight !== undefined && inFlight.version >= version) return inFlight.done;
    const abort = new AbortController();
    this.refreshes.add(abort);
    const bound = this.time.setTimer(
      () =>
        abort.abort(
          this.refreshFailure(version, `no view reached it within ${this.refreshTimeoutMs}ms`),
        ),
      this.refreshTimeoutMs,
    );
    const done = this.catchUp(version, abort.signal).finally(() => {
      this.time.clearTimer(bound);
      this.refreshes.delete(abort);
      if (this.refreshing?.done === done) this.refreshing = undefined;
    });
    this.refreshing = { version, done };
    return done;
  }

  /**
   * Stop following the watch and settle every refresh still pending (they reject with the
   * retriable `staleView` rejection, as a refresh that timed out does); no timer or waiter
   * of this service outlives it.
   */
  stop(): void {
    this.stopped = true;
    this.unsubscribe();
    for (const abort of [...this.refreshes]) {
      abort.abort(this.refreshFailure(this.current().version, "the service has stopped"));
    }
  }

  private refreshFailure(version: number, why: string): RejectionError {
    return new RejectionError(
      `membership view could not be refreshed to ${version}: ${why}`,
      "staleView",
    );
  }

  private async catchUp(version: number, signal: AbortSignal): Promise<void> {
    if (await this.reached(version, this.refreshWatchGraceMs, signal)) return;
    if (this.watch.relist !== undefined) {
      const listRevision = await settledOrAborted(
        this.watch.relist().catch(() => undefined),
        signal,
      );
      const current = this.current();
      if (current.version >= version) return;
      if (listRevision !== undefined && listRevision >= version) {
        this.publish({ version, silos: current.silos });
        return;
      }
    }
    await this.reached(version, undefined, signal);
  }

  /**
   * Whether the view reaches `version` within `timeoutMs` (forever when `undefined`); rejects
   * with the signal's reason when it aborts. Its timer and waiter are gone when it settles.
   */
  private reached(
    version: number,
    timeoutMs: number | undefined,
    signal: AbortSignal,
  ): Promise<boolean> {
    if (this.current().version >= version) return Promise.resolve(true);
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise<boolean>((resolve, reject) => {
      let timer: ReturnType<TimeProvider["setTimer"]> | undefined;
      const waiter = { version, wake: () => finish(true) };
      const onAbort = (): void => {
        cleanup();
        reject(signal.reason);
      };
      const cleanup = (): void => {
        if (timer !== undefined) this.time.clearTimer(timer);
        this.viewWaiters.delete(waiter);
        signal.removeEventListener("abort", onAbort);
      };
      const finish = (value: boolean): void => {
        cleanup();
        resolve(value);
      };
      this.viewWaiters.add(waiter);
      signal.addEventListener("abort", onAbort, { once: true });
      if (timeoutMs !== undefined) timer = this.time.setTimer(() => finish(false), timeoutMs);
    });
  }

  /** Publish a view and wake the refreshes waiting for a version it reaches. */
  private publish(snapshot: MembershipSnapshot): void {
    this.feed.publish(snapshot);
    for (const waiter of [...this.viewWaiters]) {
      if (snapshot.version >= waiter.version) waiter.wake();
    }
  }

  private onSlices(slices: EndpointSlice[], revision: number): void {
    if (typeof revision !== "number" || !Number.isFinite(revision)) {
      throw new Error(
        "KubernetesMembership: an EndpointSlice update must carry the revision it was observed " +
          "at; one without it cannot be compared with a peer's view",
      );
    }
    const previous = this.feed.current();
    // The same revision is the same set of slice writes, so the same view: nothing to
    // publish (a re-list after a reconnect that missed nothing).
    if (this.published && revision === previous.version) return;
    const members = dedupeMembers(siloMembersFromSlices(slices, this.options.portName));
    const metadataByUid = metadataFromSlices(slices, this.options.metadataLabelPrefix);
    const silos: SiloMember[] = members.map((member) => {
      const metadata = metadataByUid.get(member.address.podUid);
      return metadata !== undefined ? { ...member, metadata } : member;
    });
    const listed = silos.some((m) => isIncarnationOf(m.address, this.local));
    if (listed && this.standing === "unlisted") this.standing = "listed";
    else if (!listed && this.standing === "listed") this.standing = "removed";
    this.published = true;
    this.publish({ version: Math.max(revision, previous.version), silos });
  }
}

/** Whether `a` and `b` name the same pod incarnation (`podName#podUid`). */
function isIncarnationOf(a: SiloAddress, b: SiloAddress): boolean {
  return a.podName === b.podName && a.podUid === b.podUid;
}

/**
 * One member per pod incarnation (`podName#podUid`): an endpoint can appear in two slices
 * while the EndpointSlice controller moves it, and the later entry wins.
 */
function dedupeMembers(members: readonly SiloMember[]): SiloMember[] {
  const seen = new Map<string, SiloMember>();
  for (const member of members) {
    seen.set(`${member.address.podName}#${member.address.podUid}`, member);
  }
  return [...seen.values()];
}

/** `promise`'s value, or a rejection with the signal's reason if it aborts first. */
function settledOrAborted<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (err: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(err);
      },
    );
  });
}
