import { newActivationId } from "@tsva/core/activation-id";
import { RejectionError } from "@tsva/core/errors";
import type { GrainAddress } from "@tsva/core/grain-address";
import type { GrainType } from "@tsva/core/grain-type";
import type { InvocationRequest } from "@tsva/core/request";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { GrainDirectory } from "@tsva/directory/grain-directory";
import type { LocationCache } from "@tsva/directory/location-cache";
import type { Catalog } from "@tsva/runtime/catalog";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
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
  /** Extra placement context (activation counts, RNG); `localSilo` is filled in. */
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
    const existing = this.deps.catalog.get(req.target);
    if (existing !== undefined && existing.state !== "invalid") return existing.invoke(req);

    const activationId = newActivationId();
    const winner = await this.deps.directory.register({
      grainId: req.target,
      silo: this.deps.local,
      activationId,
    });
    this.deps.cache.put(winner);

    if (winner.silo.equals(this.deps.local) && winner.activationId === activationId) {
      return this.deps.catalog.activateLocal(req.target, activationId).invoke(req);
    }
    return this.deps.remote.send(winner.silo, req);
  }

  private async routeTo(addr: GrainAddress, req: InvocationRequest): Promise<unknown> {
    if (!addr.silo.equals(this.deps.local)) return this.deps.remote.send(addr.silo, req);
    const act = this.deps.catalog.get(req.target);
    if (act === undefined || act.state === "invalid" || act.activationId !== addr.activationId) {
      throw new RejectionError(`no activation for ${req.target.toString()}`, "noActivation");
    }
    return act.invoke(req);
  }

  private async placeAndInvoke(req: InvocationRequest): Promise<unknown> {
    const active = this.deps.activeSilos();
    // Version-aware placement: prefer compatible silos, but fall back to the full
    // set when none qualify (best-effort — placement never fails on version).
    let candidates: readonly SiloAddress[] = active;
    if (this.deps.versionFilter !== undefined) {
      const compatible = await this.deps.versionFilter(req, active);
      if (compatible.length > 0) candidates = compatible;
    }
    const strategy = this.deps.placementFor(req.target.type);
    const targetSilo = strategy.choose(req.target.type, candidates, {
      localSilo: this.deps.local,
      ...this.deps.placementContext(),
    });
    return targetSilo.equals(this.deps.local)
      ? this.deliverLocal(req)
      : this.deps.remote.send(targetSilo, req);
  }
}

function isStaleRejection(err: unknown): boolean {
  return (
    err instanceof RejectionError && (err.kind === "noActivation" || err.kind === "unknownTarget")
  );
}
