import { newActivationId } from "@tsva/core/activation-id";
import { RejectionError } from "@tsva/core/errors";
import type { GrainAddress } from "@tsva/core/grain-address";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainType } from "@tsva/core/grain-type";
import type { InvocationRequest } from "@tsva/core/request";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { GrainDirectory } from "@tsva/directory/grain-directory";
import type { LocationCache } from "@tsva/directory/location-cache";
import {
  withActivateGrainSpan,
  withFilterPlacementCandidatesSpan,
  withPlaceGrainSpan,
  withRegisterDirectoryEntrySpan,
} from "@tsva/observability/activation-tracing";
import type { Catalog } from "@tsva/runtime/catalog";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import type { PlacementFilter } from "@tsva/runtime/placement/placement-filter";
import { resolvePlacementHint } from "@tsva/runtime/placement/placement-hint";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";

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
}

/**
 * Routes a grain call to its activation across the cluster (docs/03, docs/06):
 * location cache -> directory lookup -> placement, delivering locally or
 * forwarding over the transport. Activation is race-safe via the directory's
 * compare-and-set register: concurrent activators converge on one winner and
 * the loser forwards to it.
 */
export class DistributedDispatcher implements Dispatcher {
  constructor(private readonly deps: DistributedDispatcherDeps) {}

  async invoke(req: InvocationRequest): Promise<unknown> {
    if (this.deps.clientRouter?.isClientTarget(req.target))
      return this.deps.clientRouter.route(req);

    // [StatelessWorker] grains are placement-local (`StatelessWorkerPlacement`
    // always resolves to the calling silo) and never directory-registered —
    // they may have several interchangeable local activations per id, which
    // the single-winner directory CAS below has no way to represent. Route
    // straight to the catalog's pick-or-scale instead of the cache/directory
    // funnel; this also means a stateless-worker call always resolves on
    // whichever silo makes it, exactly like Orleans.
    if (this.deps.catalog.isStatelessWorkerType(req.target.type)) {
      return this.deps.catalog.pickOrScaleWorker(req.target).invoke(req);
    }

    const cached = this.deps.cache.get(req.target);
    if (cached !== undefined) {
      try {
        return await this.routeTo(cached, req);
      } catch (err) {
        if (!isStaleRejection(err)) throw err;
        this.deps.cache.invalidate(req.target); // stale entry: re-resolve below
      }
    }

    const found = await this.deps.directory.lookup(req.target);
    if (found !== undefined) {
      this.deps.cache.put(found);
      return this.routeTo(found, req);
    }

    return this.placeAndInvoke(req);
  }

  /** A request that arrived here: ensure a local activation, or forward to the CAS winner. */
  async deliverLocal(req: InvocationRequest): Promise<unknown> {
    const existing = await this.deps.catalog.resolveLive(req.target);
    if (existing !== undefined) return existing.invoke(req);

    // Placement + directory registration + (if we won) activation all run
    // inside a "place grain" span (Runtime source), so they share the trace
    // id of whatever extracted the incoming call's trace context (see
    // `ClusterNode.receiveRequest`). Nested inside it, "activate grain"
    // (Lifecycle source) wraps registration through activation, so
    // "register directory entry" (Runtime source) is a child span of it.
    return withPlaceGrainSpan(() => this.claimAndActivateLocally(req));
  }

  /**
   * Same as `deliverLocal`, but without its own "place grain" span — for use
   * from inside `placeAndInvoke`, which already owns one that wraps the
   * filter/strategy decision too (Orleans `PlaceGrainAsync`: one span covers
   * filtering through activation, not two nested ones).
   */
  private async deliverLocalWithinPlacementSpan(req: InvocationRequest): Promise<unknown> {
    const existing = await this.deps.catalog.resolveLive(req.target);
    if (existing !== undefined) return existing.invoke(req);
    return this.claimAndActivateLocally(req);
  }

  private async claimAndActivateLocally(req: InvocationRequest): Promise<unknown> {
    const activationId = newActivationId();
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
        return this.activateLocalAndInvoke(req, activationId);
      }

      // The directory points back at this silo but at a different activation id.
      if (winner.silo.equals(this.deps.local)) {
        const act = this.deps.catalog.get(req.target);
        // A concurrent activator won the race here and is coming up: defer to it.
        if (
          act !== undefined &&
          act.state !== "invalid" &&
          act.activationId === winner.activationId
        ) {
          return act.invoke(req);
        }
        // The entry points at a dead/absent local activation (a failed activation
        // its owner has not yet unregistered, or a failed migration). Remove the
        // stale pointer and re-resolve locally instead of forwarding to ourselves
        // forever — which is what otherwise drives an always-failing activation
        // into an unbounded self-forward loop (OOM).
        await this.deps.directory.unregister(winner);
        this.deps.cache.invalidate(req.target);
        return this.deliverLocal(req);
      }

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
   */
  private async activateLocalAndInvoke(
    req: InvocationRequest,
    activationId: string,
  ): Promise<unknown> {
    let act: Awaited<ReturnType<Catalog["activateLocal"]>> | undefined;
    try {
      act = await this.deps.catalog.activateLocal(req.target, activationId);
      return await act.invoke(req);
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

  private async routeTo(addr: GrainAddress, req: InvocationRequest): Promise<unknown> {
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
      return this.deliverLocal(req);
    }
    return act.invoke(req);
  }

  private async placeAndInvoke(req: InvocationRequest): Promise<unknown> {
    // The whole placement decision — filtering, strategy choice, and (if we
    // land locally) directory registration + activation — runs inside one
    // "place grain" span (Runtime source), matching Orleans' `PlaceGrainAsync`
    // and giving `FilterPlacementCandidates`/`ActivateGrain`/
    // `RegisterDirectoryEntry` a common parent.
    return withPlaceGrainSpan(async () => {
      const ctx: PlacementContext = { localSilo: this.deps.local, ...this.deps.placementContext() };
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
        ? this.deliverLocalWithinPlacementSpan(req)
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
