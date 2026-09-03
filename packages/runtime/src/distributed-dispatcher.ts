import { newActivationId } from "@thresh/core/activation-id";
import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { type Logger, noopLogger } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import type { LocationCache } from "@thresh/directory/location-cache";
import {
  withActivateGrainSpan,
  withFilterPlacementCandidatesSpan,
  withPlaceGrainSpan,
  withRegisterDirectoryEntrySpan,
} from "@thresh/observability/activation-tracing";
import type { Catalog } from "@thresh/runtime/catalog";
import {
  withCallDeadline,
  type Dispatcher,
  type InvokeCallOptions,
} from "@thresh/runtime/dispatcher";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import { dispatchDetachingOneWay } from "@thresh/runtime/one-way-dispatch";
import { resolvePlacementHint } from "@thresh/runtime/placement/placement-hint";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@thresh/runtime/placement/placement-strategy";

/** Sends a request to a remote silo and awaits its response. */
export interface RemoteInvoker {
  send(silo: SiloAddress, req: InvocationRequest): Promise<unknown>;
}

export interface DistributedDispatcherDeps {
  local: SiloAddress;
  directory: GrainDirectory;
  cache: LocationCache;
  catalog: Catalog;
  remote: RemoteInvoker;
  activeSilos: () => SiloAddress[];
  placementFor: (grainType: GrainType) => PlacementStrategy;
  /** Filters that prune candidate silos by metadata before the strategy chooses. */
  filtersFor: (grainType: GrainType) => readonly PlacementFilter[];
  /** Extra placement context (activation counts, metadata, RNG); `localSilo` is filled in. */
  placementContext: () => Omit<PlacementContext, "localSilo">;
  /**
   * Optional version-aware pre-filter (grain-interface versioning). Wired only
   * when versioning is active; returns the compatible subset of `candidates`
   * (possibly empty). When absent, placement uses the full candidate set as
   * before.
   */
  versionFilter?: (
    req: InvocationRequest,
    candidates: readonly SiloAddress[],
  ) => Promise<readonly SiloAddress[]>;
  /**
   * Optional diversion for calls targeting a client-hosted observer
   * (`$client` grain type): routed to the client's gateway instead of the
   * grain-directory/placement funnel below. Absent for dispatchers with no
   * client-routing concept.
   */
  clientRouter?: {
    isClientTarget(target: GrainId): boolean;
    route(req: InvocationRequest): Promise<unknown>;
  };
  /**
   * Optional hook (wired only when the activation repartitioner is enabled)
   * recording the communication edge from this (calling) silo's perspective,
   * at the moment a call's target silo is resolved — before it dispatches
   * locally or over the wire. See
   * `ActivationRepartitioner.recordLocalEdge`.
   */
  recordEdge?: (req: InvocationRequest, targetSilo: SiloAddress) => void;
  /**
   * Where a detached `oneWay` delivery's failure is reported (see
   * `dispatchDetachingOneWay`). Optional so a test can wire a dispatcher
   * without one; defaults to `noopLogger`.
   */
  logger?: Logger;
}

/**
 * Routes a grain call to its activation across the cluster (docs/03, docs/06):
 * location cache -> directory lookup -> placement, delivering locally or
 * forwarding over the transport. Activation is race-safe via the directory's
 * compare-and-set register: concurrent activators converge on one winner and
 * the loser forwards to it.
 */
export class DistributedDispatcher implements Dispatcher {
  /**
   * Grain ids with a ONE-WAY local activation claim in flight, keyed by
   * `GrainId.toString()`; the promise resolves once that claimant has queued
   * its own turn (or established that no local activation is coming).
   *
   * Orleans never lets two claims for one grain id run side by side within a
   * silo: `Catalog.GetOrCreateActivation` installs the (not yet running)
   * `ActivationData` in the activation directory synchronously, so every later
   * message for that id queues on THAT activation's waiting queue in arrival
   * order instead of independently re-running placement and the directory
   * register. `Catalog.getOrActivate` reproduces exactly that for the
   * single-silo path by being deliberately non-`async` (see its comment), but
   * the distributed claim cannot: it must `await` the directory CAS before it
   * may create the activation, so two callers can both see
   * `resolveLive() === undefined` and both register, and the CAS loser can
   * then reach `activation.invoke` BEFORE the winner does.
   *
   * That used to be unreachable from one caller's own code — a caller held its
   * whole delivery, so its next call could not even start — and detaching
   * `oneWay` deliveries made it the normal case for successive fire-and-forget
   * calls to a cold grain, silently reordering them. Hence the gate, and hence
   * its scope: it serializes the claims that detaching set racing, and leaves
   * an ordinary (awaited) call's claim to race exactly as it always has, since
   * imposing an order there would change scheduling for callers that never
   * asked for it.
   */
  private readonly claimingOneWay = new Map<string, Promise<void>>();

  /**
   * Activation ids currently being brought up on this silo, whatever kind of
   * call claimed them; the promise resolves at the same point as above.
   *
   * Read by the CAS loser before it treats a local directory entry as stale:
   * "the directory names a local activation the catalog does not have" means
   * a dead or abandoned entry ONLY if nobody is in the middle of creating that
   * activation right now. Without this check a loser that resumes before the
   * winner has called `Catalog.activateLocal` unregisters the winner's brand
   * new entry, and the winner then runs with no directory registration at all
   * — so any other silo places a SECOND activation of the same grain, which is
   * precisely the invariant that branch exists to protect.
   */
  private readonly comingUpLocally = new Map<string, Promise<void>>();

  constructor(private readonly deps: DistributedDispatcherDeps) {}

  invoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    // A `oneWay` call resolves to the caller as soon as delivery is under way
    // — whether the callee turns out to live here or on another silo, which is
    // what makes `fireAndForget()` placement-independent. `deliver` still runs
    // the whole funnel (cache, directory, placement, forward, local turn) to
    // completion; the caller just isn't holding its promise.
    return dispatchDetachingOneWay(req, this.deps.logger ?? noopLogger, () =>
      this.deliver(req, opts),
    );
  }

  private async deliver(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    // Resolved once, up front, so a fresh `opts.deadlineMs` is embedded into
    // `req.deadline` before any placement/forwarding decision below — it must
    // ride a distributed forward too, but `opts.signal` itself never can (see
    // `InvokeCallOptions`), so it is threaded only through the local-delivery
    // paths below and dropped at every `remote.send`.
    const withDeadline = withCallDeadline(req, opts);

    if (this.deps.clientRouter?.isClientTarget(withDeadline.target))
      return this.deps.clientRouter.route(withDeadline);

    // [StatelessWorker] grains are placement-local (`StatelessWorkerPlacement`
    // always resolves to the calling silo) and never directory-registered —
    // they may have several interchangeable local activations per id, which
    // the single-winner directory CAS below has no way to represent. Route
    // straight to the catalog's pick-or-scale instead of the cache/directory
    // funnel; this also means a stateless-worker call always resolves on
    // whichever silo makes it, exactly like Orleans.
    if (this.deps.catalog.isStatelessWorkerType(withDeadline.target.type)) {
      return this.deps.catalog.pickOrScaleWorker(withDeadline.target).invoke(withDeadline, opts);
    }

    const cached = this.deps.cache.get(withDeadline.target);
    if (cached !== undefined) {
      try {
        return await this.routeTo(cached, withDeadline, opts);
      } catch (err) {
        if (!isStaleRejection(err)) throw err;
        this.deps.cache.invalidate(withDeadline.target); // stale entry: re-resolve below
      }
    }

    const found = await this.deps.directory.lookup(withDeadline.target);
    if (found !== undefined) {
      this.deps.cache.put(found);
      return this.routeTo(found, withDeadline, opts);
    }

    return this.placeAndInvoke(withDeadline, opts);
  }

  /** A request that arrived here: ensure a local activation, or forward to the CAS winner. */
  async deliverLocal(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    const existing = await this.deps.catalog.resolveLive(req.target);
    if (existing !== undefined) return existing.invoke(req, opts);

    // Placement + directory registration + (if we won) activation all run
    // inside a "place grain" span (Runtime source), so they share the trace
    // id of whatever extracted the incoming call's trace context (see
    // `ClusterNode.receiveRequest`). Nested inside it, "activate grain"
    // (Lifecycle source) wraps registration through activation, so
    // "register directory entry" (Runtime source) is a child span of it.
    return withPlaceGrainSpan(() => this.claimAndActivateLocally(req, opts));
  }

  /**
   * Same as `deliverLocal`, but without its own "place grain" span — for use
   * from inside `placeAndInvoke`, which already owns one that wraps the
   * filter/strategy decision too (Orleans `PlaceGrainAsync`: one span covers
   * filtering through activation, not two nested ones).
   */
  private async deliverLocalWithinPlacementSpan(
    req: InvocationRequest,
    opts?: InvokeCallOptions,
  ): Promise<unknown> {
    const existing = await this.deps.catalog.resolveLive(req.target);
    if (existing !== undefined) return existing.invoke(req, opts);
    return this.claimAndActivateLocally(req, opts);
  }

  /**
   * Book-keeping around one local activation claim: a one-way claim queues
   * behind any one-way claim already in flight for the same grain id (see
   * `claimingOneWay`), and every claim publishes its activation id as
   * "coming up here" for as long as it takes to queue its turn (see
   * `comingUpLocally`).
   */
  private async claimAndActivateLocally(
    req: InvocationRequest,
    opts?: InvokeCallOptions,
  ): Promise<unknown> {
    const key = req.target.toString();
    const oneWay = req.options.oneWay === true;
    while (oneWay) {
      const inFlight = this.claimingOneWay.get(key);
      if (inFlight === undefined) break;
      await inFlight;
      const act = await this.deps.catalog.resolveLive(req.target);
      // The claim we waited on brought the activation up here: queue this call
      // on it, behind the claimant's own turn.
      if (act !== undefined) return act.invoke(req, opts);
      // It produced no local activation (the CAS was won by another silo, or
      // the activation failed to come up), so claim in our own right — but
      // re-check the gate first, since another caller may have started one in
      // the meantime. This loops only while some OTHER claim is in flight, and
      // every claim clears its entry, so it cannot spin on itself.
    }

    const activationId = newActivationId();
    let claimed!: () => void;
    const gate = new Promise<void>((resolve) => (claimed = resolve));
    if (oneWay) this.claimingOneWay.set(key, gate);
    this.comingUpLocally.set(activationId, gate);
    // Idempotent, and called as soon as this claim's own turn is queued (NOT
    // when that turn completes): a later call must be free to interleave with
    // a running turn exactly as it would on an already-live activation.
    const admitted = (): void => {
      if (this.claimingOneWay.get(key) === gate) this.claimingOneWay.delete(key);
      this.comingUpLocally.delete(activationId);
      claimed();
    };
    try {
      return await this.claimLocalActivation(req, activationId, admitted, opts);
    } finally {
      admitted();
    }
  }

  private async claimLocalActivation(
    req: InvocationRequest,
    activationId: string,
    admitted: () => void,
    opts?: InvokeCallOptions,
  ): Promise<unknown> {
    return withActivateGrainSpan({ grainType: req.target.type }, async () => {
      const winner = await withRegisterDirectoryEntrySpan({ grainId: req.target.toString() }, () =>
        this.deps.directory.register({
          grainId: req.target,
          silo: this.deps.local,
          activationId,
        }),
      );
      this.deps.cache.put(winner);

      // We won the CAS: activate here.
      if (winner.silo.equals(this.deps.local) && winner.activationId === activationId) {
        return this.activateLocalAndInvoke(req, activationId, admitted, opts);
      }

      // The directory points back at this silo but at a different activation id.
      if (winner.silo.equals(this.deps.local)) {
        const act = this.deps.catalog.get(req.target);
        // An activation under that id is already here (a migration that landed
        // while we were resolving, or an entry an earlier claim left): defer to it.
        if (
          act !== undefined &&
          act.state !== "invalid" &&
          act.activationId === winner.activationId
        ) {
          const turn = act.invoke(req, opts);
          admitted();
          return await turn;
        }
        // Not here YET, rather than not here at all: the CAS winner is a claim
        // on this silo that has not reached `Catalog.activateLocal`. Wait for
        // it and re-resolve — tearing its registration down (below) would leave
        // the activation it is about to create unregistered, and every other
        // silo would then place a second one. See `comingUpLocally`.
        const comingUp = this.comingUpLocally.get(winner.activationId);
        if (comingUp !== undefined) {
          admitted();
          await comingUp;
          return this.deliverLocal(req, opts);
        }
        // The entry points at a dead/absent local activation (a failed activation
        // its owner has not yet unregistered, or a failed migration). Remove the
        // stale pointer and re-resolve locally instead of forwarding to ourselves
        // forever — which is what otherwise drives an always-failing activation
        // into an unbounded self-forward loop (OOM). Release this claim first:
        // `deliverLocal` re-enters the claim path, and a caller waiting on our
        // gate is better off re-resolving than queueing behind a repair it
        // cannot see.
        admitted();
        await this.deps.directory.unregister(winner);
        this.deps.cache.invalidate(req.target);
        return this.deliverLocal(req, opts);
      }

      admitted(); // the grain lives elsewhere: nothing is coming up here
      return this.deps.remote.send(winner.silo, req);
    });
  }

  /**
   * Activate the grain locally under the id we won and run the call. If the
   * activation itself fails to come up (its constructor or `onActivate` threw),
   * remove the directory registration we won so subsequent calls re-resolve to a
   * fresh placement rather than looping against a dead entry — mirroring Orleans,
   * which reports the activation failure to the caller and discards the
   * activation (it is not retried within the call). The original error is
   * rethrown so the caller sees it; an ordinary method error after a successful
   * activation leaves the (still valid) activation and its registration intact.
   *
   * `admitted` is signalled the instant this call's turn is on the activation's
   * scheduler queue — `ActivationData.invoke` enqueues synchronously, before
   * its first `await` — so a caller waiting on the claim gate queues strictly
   * behind it (see `claimingOneWay`).
   */
  private async activateLocalAndInvoke(
    req: InvocationRequest,
    activationId: string,
    admitted: () => void,
    opts?: InvokeCallOptions,
  ): Promise<unknown> {
    let act: Awaited<ReturnType<Catalog["activateLocal"]>> | undefined;
    try {
      act = await this.deps.catalog.activateLocal(req.target, activationId);
      const turn = act.invoke(req, opts);
      admitted();
      return await turn;
    } catch (err) {
      if (act === undefined || act.activationFailed) {
        await this.deps.directory.unregister({
          grainId: req.target,
          silo: this.deps.local,
          activationId,
        });
        this.deps.cache.invalidate(req.target);
      }
      throw err;
    }
  }

  private async routeTo(
    addr: GrainAddress,
    req: InvocationRequest,
    opts?: InvokeCallOptions,
  ): Promise<unknown> {
    this.deps.recordEdge?.(req, addr.silo);
    if (!addr.silo.equals(this.deps.local)) return this.deps.remote.send(addr.silo, req);
    const act = await this.deps.catalog.resolveLive(req.target);
    if (act === undefined || act.activationId !== addr.activationId) {
      // The cache/directory points here but no live activation matches (a failed
      // or collected activation, or a stale pointer). Rather than rejecting —
      // which, when the caller is this same silo, has nothing to re-resolve
      // against and loops forever against the dead entry — (re)activate locally.
      // `deliverLocal` registers a fresh activation, repairs a stale self-pointer,
      // or forwards if the grain in fact lives on another silo.
      return this.deliverLocal(req, opts);
    }
    return act.invoke(req, opts);
  }

  private async placeAndInvoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    // The whole placement decision — filtering, strategy choice, and (if we
    // land locally) directory registration + activation — runs inside one
    // "place grain" span (Runtime source), matching Orleans' `PlaceGrainAsync`
    // and giving `FilterPlacementCandidates`/`ActivateGrain`/
    // `RegisterDirectoryEntry` a common parent.
    return withPlaceGrainSpan(async () => {
      // `grainId` last: the dispatcher knows the grain being placed, and a context provider
      // must not be able to name a different one.
      const ctx: PlacementContext = {
        localSilo: this.deps.local,
        ...this.deps.placementContext(),
        grainId: req.target,
      };
      // Hard metadata filters first: prune the candidate set by silo metadata; if
      // none qualify, placement fails (the grain has a constraint nothing satisfies).
      let candidates: readonly SiloAddress[] = this.deps.activeSilos();
      for (const filter of this.deps.filtersFor(req.target.type)) {
        candidates = withFilterPlacementCandidatesSpan(
          { filterType: filter.constructor.name },
          () => filter.filter(req.target.type, candidates, ctx),
        );
      }
      if (candidates.length === 0) {
        throw new RejectionError(
          `no placement candidates for ${req.target.type} after filtering`,
          "noCandidates",
        );
      }
      // Version-aware placement: prefer compatible silos within the eligible set,
      // but fall back to it when none qualify (best-effort — never fails on version).
      if (this.deps.versionFilter !== undefined) {
        const compatible = await this.deps.versionFilter(req, candidates);
        if (compatible.length > 0) candidates = compatible;
      }
      // Directed placement: a caller-set RequestContext hint (Orleans
      // `IPlacementDirector.PlacementHintKey`) wins over the strategy when it
      // names a live candidate; otherwise fall through to the strategy as before.
      const strategy = this.deps.placementFor(req.target.type);
      const targetSilo =
        resolvePlacementHint(req.headers, candidates) ??
        strategy.choose(req.target.type, candidates, ctx);
      this.deps.recordEdge?.(req, targetSilo);
      return targetSilo.equals(this.deps.local)
        ? this.deliverLocalWithinPlacementSpan(req, opts)
        : this.deps.remote.send(targetSilo, req);
    });
  }
}

function isStaleRejection(err: unknown): boolean {
  return (
    err instanceof RejectionError &&
    (err.kind === "noActivation" || err.kind === "unknownTarget" || err.kind === "staleView")
  );
}
