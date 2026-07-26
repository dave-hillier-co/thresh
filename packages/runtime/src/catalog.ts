import * as os from "node:os";
import { GrainCallError } from "@thresh/core/errors";
import type { Grain } from "@thresh/core/grain";
import type { IncomingGrainCallFilter } from "@thresh/core/grain-call-filter";
import type { BroadcastChannelProvider } from "@thresh/core/broadcast-channel";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainMetadata } from "@thresh/core/grain-metadata";
import type { GrainType } from "@thresh/core/grain-type";
import type { DeactivationReason } from "@thresh/core/reasons";
import type { ReminderRegistry } from "@thresh/core/reminder";
import type { DurableJobScheduler } from "@thresh/core/durable-job";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { StreamProvider } from "@thresh/core/stream";
import { ActivationData, type ActivationOptions } from "@thresh/runtime/activation";
import {
  cancellationExtensionFactory,
  ICancellationSourcesExtension,
} from "@thresh/runtime/cancellation-extension";
import type { GrainFactory } from "@thresh/runtime/grain-factory";
import {
  grainManagementExtensionFactory,
  IGrainManagementExtension,
} from "@thresh/runtime/grain-management-extension";
import { GrainRuntimeImpl } from "@thresh/runtime/grain-runtime-impl";
import type { GrainServiceRegistry } from "@thresh/runtime/grain-service";
import type { SiloLoadSheddingTestHooks } from "@thresh/runtime/load-shedding";
import type { TimeProvider } from "@thresh/runtime/time-provider";

export interface RegisteredGrain {
  ctor: new () => Grain;
  metadata: GrainMetadata;
}

/**
 * Default `[StatelessWorker]` scaling ceiling when a grain type sets
 * `stateless: true` but no explicit `maxLocalWorkers` — mirrors Orleans'
 * `Environment.ProcessorCount` default. Falls back to a small constant if the
 * host reports no CPUs (unusual, but `os.cpus()` can legitimately return `[]`
 * in some sandboxes).
 */
const DEFAULT_MAX_LOCAL_WORKERS = os.cpus().length || 4;

/**
 * Optional hook to construct/dispose grain instances instead of `new ctor()`
 * (Orleans `IGrainActivator`) — e.g. object pooling or non-DI construction.
 */
export interface GrainActivator {
  /** Construct a grain instance instead of `new ctor()`. */
  createInstance(ctor: new () => Grain, id: GrainId): Grain;
  /** Called when an activation using this activator is deactivated (idle collection or explicit). */
  disposeInstance?(instance: Grain, id: GrainId): void | Promise<void>;
}

export interface CatalogOptions {
  grainTypes: ReadonlyMap<GrainType, RegisteredGrain>;
  factory: GrainFactory;
  time: TimeProvider;
  defaultCollectionAgeSeconds: number;
  /**
   * Optional hook to construct/dispose grain instances instead of `new ctor()` —
   * e.g. object pooling or non-DI construction. Defaults to `new ctor()` when unset.
   */
  grainActivator?: GrainActivator;
  /** Called after an activation has been deactivated (idle collection or shutdown). */
  onDeactivated?: (activation: ActivationData) => void;
  /**
   * Bind state before `onActivate` (provided by the hosting layer). In `"activate"`
   * mode it reads from storage; in `"rehydrate"` mode it binds the facets without
   * reading, so migrated state restored from the bag is not clobbered.
   */
  activateState?: (
    instance: Grain,
    grainId: GrainId,
    mode?: "activate" | "rehydrate",
  ) => Promise<void>;
  /**
   * Unbind state after deactivation (provided by the hosting layer), the
   * mirror image of {@link activateState} — e.g. stopping a transactional-state
   * facet's confirmation-worker timer so an idle activation doesn't keep it
   * running. Called once per deactivated activation (idle collection, an
   * explicit `deactivateOnIdle()`, or silo shutdown), after `onDeactivate` has
   * run and the activation is finalized.
   */
  deactivateState?: (instance: Grain, grainId: GrainId) => void;
  /** Migrate an idle activation to another silo; resolves to whether it moved. */
  migrate?: (activation: ActivationData) => Promise<boolean>;
  /** Resolves the reminder registry a grain's `registerReminder` delegates to. */
  reminderRegistry?: () => ReminderRegistry | undefined;
  /** Resolves the durable-job scheduler a grain's `scheduleJob` delegates to. */
  durableJobScheduler?: () => DurableJobScheduler | undefined;
  /** Resolves the stream provider a grain's `getStreamProvider` returns. */
  streamProvider?: (name?: string) => StreamProvider | undefined;
  /** Resolves the broadcast-channel provider a grain's `getBroadcastChannelProvider` returns. */
  broadcastProvider?: (name?: string) => BroadcastChannelProvider | undefined;
  /** Resolves this silo's own address, for a grain's `runtime.localSiloAddress()`. */
  localSilo?: () => SiloAddress | undefined;
  /** Resolves this silo's load-shedding test hooks, for a grain's `runtime.latchCpuUsage()`-style methods. */
  loadShedding?: () => SiloLoadSheddingTestHooks | undefined;
  /** Pings a specific silo's control target, for a grain's `runtime.pingSilo()`. */
  siloPing?: (siloAddress: SiloAddress, message?: string) => Promise<void>;
  /** Resolves this silo's grain-service registry, for a grain's `runtime.getGrainService()`. */
  grainServiceRegistry?: () => GrainServiceRegistry | undefined;
  /** Incoming call filters wrapping each grain-method dispatch (silo-wide). */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
  /**
   * Dev-mode `@readOnly` mutation guard (`GAP-READONLY-ENFORCEMENT`): when
   * `true`, every activation's persistent, reducer, durable-journaling and
   * transactional state facets are proxy-guarded for the duration of a
   * `readOnly` turn, rejecting a mutation attempt with
   * `ReadOnlyStateViolationError`. Opt-in and `false` by default (production
   * default is off — the silo builder is the place to flip it on for dev/test).
   */
  readOnlyStateGuard?: boolean;
  /**
   * Auto-install factories for `GrainExtension` interfaces, keyed by interface
   * id (Orleans `AddGrainExtension`) — set on the silo builder and shared
   * unmodified across every activation. Absent means no auto-install: an
   * un-bound extension call always throws `GrainExtensionNotInstalledException`.
   */
  grainExtensionFactories?: ReadonlyMap<number, () => object>;
  /**
   * How a cascading `cancelRemoteToken` reaches a FORWARDED target's own
   * activation (see `CancellationSourcesExtension.recordForwardTarget`),
   * cluster-wide — set by `ClusterNode` to route over the dispatcher.
   * Defaults to a no-op: a standalone catalog with no cluster to route to
   * still gets single-hop cancellation (the original, un-cascaded
   * behaviour), it just can't cascade past this silo.
   */
  cancellationCanceller?: (target: GrainId, tokenId: string) => Promise<void>;
  /**
   * Turn-scheduler back-pressure/watchdog and deactivation-timeout knobs
   * (Orleans `SchedulingOptions` + `CollectionOptions.DeactivationTimeout`),
   * applied to every activation this catalog creates. Unset (the default)
   * keeps every activation's queue unbounded and its deactivation untimed —
   * the silo builder is what actually turns these on with Orleans-like
   * defaults for a hosted silo.
   */
  activationOptions?: ActivationOptions;
}

/** Registry of live activations on this silo, keyed by grain id. */
export class Catalog {
  private readonly activations = new Map<string, ActivationData>();
  /**
   * `[StatelessWorker]` grain ids may have MULTIPLE local activations
   * (Orleans: interchangeable, scaled on demand, never directory-registered
   * — see `StatelessWorkerPlacement`), so they get a parallel list-per-id
   * structure instead of sharing `activations` (which is exactly one
   * activation per id and must stay that way for every other grain type —
   * the single-activation invariant the directory's CAS register and the
   * rest of this class depend on). Only ids of a grain type registered with
   * `stateless: true` are ever stored here; see `pickOrScaleWorker`.
   */
  private readonly workerActivations = new Map<string, ActivationData[]>();
  /**
   * `options.grainExtensionFactories` adapted once to the `(activation) =>
   * object` signature `ActivationData.setExtensionFactories` expects (the
   * user-facing factory ignores the activation argument), merged with the
   * built-in cancellation extension — which every activation gets
   * regardless of user config, so `cancelRemoteToken` auto-installs on any
   * activation rather than throwing `GrainExtensionNotInstalledException`
   * (see `cancellation-extension.ts`). Built once here and shared,
   * unmodified, by every activation this catalog creates.
   */
  private readonly extensionFactories: Map<number, (activation: ActivationData) => object>;

  constructor(private readonly options: CatalogOptions) {
    this.extensionFactories = new Map(
      [...(options.grainExtensionFactories ?? [])].map(
        ([id, factory]) => [id, () => factory()] as const,
      ),
    );
    const canceller = options.cancellationCanceller ?? (async () => {});
    this.extensionFactories.set(ICancellationSourcesExtension.id, () =>
      cancellationExtensionFactory(canceller),
    );
    this.extensionFactories.set(IGrainManagementExtension.id, (activation) =>
      grainManagementExtensionFactory(activation),
    );
  }

  /** Single-silo path (Phase 1): create with a fresh activation id if absent. */
  getOrCreate(id: GrainId): Promise<ActivationData> {
    return this.getOrActivate(id);
  }

  /**
   * Multi-silo path: activate with the activation id the dispatcher won via
   * directory CAS. Returns an existing live activation if one is already here.
   */
  activateLocal(id: GrainId, activationId: string): Promise<ActivationData> {
    return this.getOrActivate(id, activationId);
  }

  /**
   * Migration path: create an activation that restores its state from `bag` (via
   * each participant's `onRehydrate`) instead of reading storage, with the
   * activation id the source silo chose for the move.
   */
  activateMigrated(
    id: GrainId,
    activationId: string,
    bag: Record<string, unknown>,
    sourceAddr?: GrainAddress,
  ): Promise<ActivationData> {
    return this.getOrActivate(id, activationId, bag, sourceAddr);
  }

  /**
   * Lookup-create-store: return a live activation if present, otherwise
   * create, store, and return one.
   *
   * Deliberately NOT declared `async`: the common case (existing live
   * activation with no pending deactivation, or nothing yet — straight to
   * `create`) must run to completion in one synchronous slice, with the
   * check-then-set on `this.activations` uninterrupted by a microtask hop.
   * `async function` bodies yield at their first `await` even when awaiting
   * an already-resolved value, which would let two calls for the same
   * not-yet-created id both pass the "doesn't exist" check before either
   * created + stored one — Promise.all([g.increment(1), g.increment(2), ...])
   * would then activate the SAME grain id multiple times, splitting the
   * calls across separate activations instead of serializing them onto one.
   * Only the rare stale-pending-deactivation branch needs to await anything,
   * so it alone is split into `finalizeStaleThenCreate`.
   */
  private getOrActivate(
    id: GrainId,
    activationId?: string,
    rehydrationBag?: Record<string, unknown>,
    sourceAddr?: GrainAddress,
  ): Promise<ActivationData> {
    const key = id.toString();
    const existing = this.activations.get(key);
    if (existing !== undefined && existing.state !== "invalid") {
      if (!existing.deactivationRequestedAndIdle) return Promise.resolve(existing);
      return this.finalizeStale(key, existing).then(() => {
        const created = this.create(id, activationId, rehydrationBag, sourceAddr);
        this.activations.set(key, created);
        return created;
      });
    }
    const created = this.create(id, activationId, rehydrationBag, sourceAddr);
    this.activations.set(key, created);
    return Promise.resolve(created);
  }

  /**
   * Finalize a PENDING `deactivateOnIdle()` request
   * (`ActivationData.deactivationRequestedAndIdle`) — observed lazily, on the
   * next lookup for this grain id, rather than synchronously inside the
   * extension call itself (see `grain-management-extension.ts` for why: the
   * extension call is still running as a turn when it flags the request, so
   * running `onDeactivate` there would deadlock against its own turn). By
   * the time the NEXT call for this id reaches `getOrActivate`/`resolveLive`,
   * that `alwaysInterleave` extension turn has already completed and the
   * scheduler is idle, so finalizing here is safe and, crucially, race-free:
   * the very next call after `deactivateOnIdle()` resolves is guaranteed to
   * find this activation gone. Runs `onDeactivate` and removes the map
   * entry, but does NOT create a replacement — callers each do that their
   * own way (`getOrActivate` creates one directly with the caller's chosen
   * activation id; `DistributedDispatcher.deliverLocal`, via `resolveLive`,
   * instead re-registers with the directory first so the replacement gets a
   * freshly CAS-won activation id, exactly like activating from scratch).
   */
  private async finalizeStale(key: string, existing: ActivationData): Promise<void> {
    await existing.runDeactivateHook({
      code: "application-requested",
      description: "deactivateOnIdle requested",
    });
    existing.finalizeDeactivation();
    this.activations.delete(key);
    if (this.options.grainActivator?.disposeInstance !== undefined) {
      await this.options.grainActivator.disposeInstance(existing.instance, existing.id);
    }
    this.options.deactivateState?.(existing.instance, existing.id);
    this.options.onDeactivated?.(existing);
  }

  /**
   * Return the live (non-invalid) activation for `id` if one exists, WITHOUT
   * creating one — called directly by `DistributedDispatcher`
   * (`routeTo`/`deliverLocal`), which resolve an already-cached/directory-
   * known address straight to `Catalog.get` and invoke it, bypassing
   * `getOrCreate`/`activateLocal` entirely once an activation exists.
   * Without this, that fast path would never observe a pending
   * `deactivateOnIdle()` request (see `finalizeStale`) and would keep
   * invoking the stale activation forever.
   */
  resolveLive(id: GrainId): Promise<ActivationData | undefined> {
    const key = id.toString();
    const existing = this.activations.get(key);
    if (existing === undefined || existing.state === "invalid") return Promise.resolve(undefined);
    if (!existing.deactivationRequestedAndIdle) return Promise.resolve(existing);
    return this.finalizeStale(key, existing).then(() => undefined);
  }

  /** A live activation for `id` — for a stateless-worker id, an arbitrary one of possibly several. */
  get(id: GrainId): ActivationData | undefined {
    const key = id.toString();
    const single = this.activations.get(key);
    if (single !== undefined) return single;
    return this.workerActivations.get(key)?.find((a) => a.state !== "invalid");
  }

  isActive(id: GrainId): boolean {
    const key = id.toString();
    if (this.activations.get(key)?.state === "valid") return true;
    return this.workerActivations.get(key)?.some((a) => a.state === "valid") ?? false;
  }

  /**
   * Live LOCAL activation count for one grain id — 0 or 1 for an ordinary
   * grain, 0..maxLocalWorkers for a stateless-worker id (Orleans
   * `IManagementGrain.GetGrainActivationCount`, per-silo half of it; the
   * management grain amalgamates this across silos itself).
   */
  countFor(id: GrainId): number {
    const key = id.toString();
    const single = this.activations.get(key);
    if (single !== undefined) return single.state === "invalid" ? 0 : 1;
    const list = this.workerActivations.get(key);
    if (list === undefined) return 0;
    let n = 0;
    for (const a of list) if (a.state !== "invalid") n++;
    return n;
  }

  /** Whether `type` was registered with `stateless: true` ([StatelessWorker]-equivalent). */
  isStatelessWorkerType(type: GrainType): boolean {
    return this.options.grainTypes.get(type)?.metadata.options.stateless === true;
  }

  /** Live activation count, used by activation-count placement. */
  count(): number {
    let n = 0;
    for (const a of this.activations.values()) if (a.state !== "invalid") n++;
    for (const list of this.workerActivations.values())
      for (const a of list) if (a.state !== "invalid") n++;
    return n;
  }

  /** Live activation count per grain type, used by the management grain's per-type statistics. */
  grainTypeCounts(): Map<GrainType, number> {
    const counts = new Map<GrainType, number>();
    const tally = (a: ActivationData): void => {
      if (a.state === "invalid") return;
      counts.set(a.id.type, (counts.get(a.id.type) ?? 0) + 1);
    };
    for (const a of this.activations.values()) tally(a);
    for (const list of this.workerActivations.values()) for (const a of list) tally(a);
    return counts;
  }

  /** Valid (servable) activations — migration candidates for the rebalancer. */
  liveActivations(): ActivationData[] {
    const out: ActivationData[] = [];
    for (const a of this.activations.values()) if (a.state === "valid") out.push(a);
    for (const list of this.workerActivations.values())
      for (const a of list) if (a.state === "valid") out.push(a);
    return out;
  }

  /** `stateless: true` grain types' configured (or defaulted) scaling ceiling, else `undefined`. */
  private maxLocalWorkersFor(type: GrainType): number | undefined {
    const reg = this.options.grainTypes.get(type);
    if (reg === undefined || reg.metadata.options.stateless !== true) return undefined;
    return reg.metadata.options.maxLocalWorkers ?? DEFAULT_MAX_LOCAL_WORKERS;
  }

  /**
   * The `[StatelessWorker]` path: choose which of `id`'s (possibly several)
   * local activations serves the next call, scaling up to `maxLocalWorkers`
   * on demand. Placement (`StatelessWorkerPlacement`) already confined the
   * call to this silo and it is never directory-registered, so this is the
   * ENTIRE routing decision — the dispatcher calls this instead of the
   * directory/CAS path for a stateless-worker grain type.
   *
   * - a non-busy existing activation, if any, is reused;
   * - otherwise, below `maxLocalWorkers`, a new one is created;
   * - otherwise (at the limit, all busy) the least-loaded existing one is
   *   picked and the call queues onto it (still completes — Orleans doesn't
   *   fail calls once at the cap, it backs up the workers it has).
   *
   * Deliberately synchronous end to end, mirroring `getOrActivate`'s
   * check-then-set discipline (see its comment): the busy-scan and the
   * create-or-pick happen in one slice with no `await` between them, so N
   * concurrent calls for the same id can't all observe "nothing free" and
   * each create a worker, overshooting `maxLocalWorkers`. Callers must invoke
   * the returned activation synchronously too, for the same reason.
   */
  pickOrScaleWorker(id: GrainId): ActivationData {
    const maxLocalWorkers = this.maxLocalWorkersFor(id.type);
    if (maxLocalWorkers === undefined) {
      throw new GrainCallError(`${id.type} is not a [StatelessWorker] grain type`);
    }
    const key = id.toString();
    let list = this.workerActivations.get(key);
    if (list === undefined) {
      list = [];
      this.workerActivations.set(key, list);
    }
    // Prune activations idle-collected or failed since the last pick — lazily,
    // here, rather than via a separate pass (mirrors how `getOrActivate`
    // lazily notices a stale single activation on the next lookup).
    for (let i = list.length - 1; i >= 0; i--) {
      if (list[i]!.state === "invalid") list.splice(i, 1);
    }
    for (const a of list) {
      if (!a.scheduler.busy) return a;
    }
    if (list.length < maxLocalWorkers) {
      const created = this.create(id);
      list.push(created);
      return created;
    }
    let best = list[0]!;
    let bestLoad = best.scheduler.load;
    for (let i = 1; i < list.length; i++) {
      const load = list[i]!.scheduler.load;
      if (load < bestLoad) {
        best = list[i]!;
        bestLoad = load;
      }
    }
    return best;
  }

  private create(
    id: GrainId,
    activationId?: string,
    rehydrationBag?: Record<string, unknown>,
    sourceAddr?: GrainAddress,
  ): ActivationData {
    const reg = this.options.grainTypes.get(id.type);
    if (reg === undefined) throw new GrainCallError(`no grain type registered: ${id.type}`);
    const ageSeconds =
      reg.metadata.options.collectionAgeSeconds ?? this.options.defaultCollectionAgeSeconds;
    const activation = new ActivationData(
      id,
      this.options.time,
      ageSeconds * 1000,
      reg.metadata.reentrant,
      activationId,
      this.options.activationOptions ?? {},
    );
    activation.runtime = new GrainRuntimeImpl(this.options.factory, activation, {
      ...(this.options.reminderRegistry !== undefined
        ? { reminders: this.options.reminderRegistry }
        : {}),
      ...(this.options.streamProvider !== undefined
        ? { streams: this.options.streamProvider }
        : {}),
      ...(this.options.broadcastProvider !== undefined
        ? { broadcastChannels: this.options.broadcastProvider }
        : {}),
      ...(this.options.durableJobScheduler !== undefined
        ? { durableJobs: this.options.durableJobScheduler }
        : {}),
      ...(this.options.localSilo !== undefined ? { localSilo: this.options.localSilo } : {}),
      ...(this.options.loadShedding !== undefined
        ? { loadShedding: this.options.loadShedding }
        : {}),
      ...(this.options.siloPing !== undefined ? { siloPing: this.options.siloPing } : {}),
      ...(this.options.grainServiceRegistry !== undefined
        ? { grainServices: this.options.grainServiceRegistry }
        : {}),
      isStatelessWorker: () => reg.metadata.options.stateless === true,
    });
    const instance =
      this.options.grainActivator !== undefined
        ? this.options.grainActivator.createInstance(reg.ctor, id)
        : new reg.ctor();
    instance.setContext(activation);
    activation.instance = instance;
    if (this.options.incomingCallFilters !== undefined) {
      activation.incomingCallFilters = this.options.incomingCallFilters;
    }
    if (this.options.readOnlyStateGuard !== undefined) {
      activation.readOnlyStateGuard = this.options.readOnlyStateGuard;
    }
    activation.setExtensionFactories(this.extensionFactories);
    const activateState = this.options.activateState;
    if (rehydrationBag !== undefined) {
      activation.rehydrationBag = rehydrationBag;
      activation.preActivate = async () => {
        if (activateState !== undefined) await activateState(instance, id, "rehydrate");
        await activation.applyRehydration(sourceAddr);
      };
      activation.beginActivate("reactivation");
    } else {
      if (activateState !== undefined) {
        activation.preActivate = () => activateState(instance, id);
      }
      activation.beginActivate("incoming-call");
    }
    return activation;
  }

  /**
   * If stale, run `onDeactivate` (honouring a migration request before or
   * during it) and finalize — leaving `activation.state === "invalid"` for
   * the caller to notice and remove from whichever map it lives in.
   * `ageLimitOverrideMs`, when given, is `ForceActivationCollection`'s
   * caller-chosen age instead of each activation's own collection age (see
   * `ActivationData.isStale`).
   */
  private async collectOne(activation: ActivationData, ageLimitOverrideMs?: number): Promise<void> {
    if (!activation.isStale(ageLimitOverrideMs)) return;
    if (activation.wantsMigration && this.options.migrate !== undefined) {
      // Faithful to Orleans' `ActivationData.FinishDeactivating`:
      // `OnDeactivateAsync` runs (and its span starts) BEFORE the dehydrate
      // span/hand-off, and the "migrating" reason is committed up front —
      // before the destination is known to have accepted the move, so it
      // doesn't retroactively change to "idle" if the move ends up failing.
      await activation.runDeactivateHook({
        code: "migrating",
        description: "migrating to another silo",
      });
      await this.options.migrate(activation);
      activation.finalizeDeactivation();
    } else {
      // No migration requested yet: run `onDeactivate` first, since a grain
      // may call `migrateOnIdle()` from within it; honour a migration it
      // asks for during the hook, then finalize.
      await activation.runDeactivateHook({ code: "idle", description: "idle collection" });
      if (activation.wantsMigration && this.options.migrate !== undefined) {
        await this.options.migrate(activation);
      }
      activation.finalizeDeactivation();
    }
  }

  private async disposeCollected(activation: ActivationData): Promise<void> {
    if (this.options.grainActivator?.disposeInstance !== undefined) {
      await this.options.grainActivator.disposeInstance(activation.instance, activation.id);
    }
    this.options.deactivateState?.(activation.instance, activation.id);
    this.options.onDeactivated?.(activation);
  }

  /**
   * Sweep every activation — ordinary and stateless-worker alike — and
   * deactivate the stale ones. `ageLimitOverrideMs` is
   * `IManagementGrain.forceActivationCollection`'s forced sweep age (e.g. `0`
   * to collect every currently-idle activation); omitted, each activation
   * uses its own configured collection age (the periodic `ActivationCollector`).
   */
  async collectIdle(ageLimitOverrideMs?: number): Promise<void> {
    for (const [key, activation] of this.activations) {
      await this.collectOne(activation, ageLimitOverrideMs);
      if (activation.state === "invalid") {
        this.activations.delete(key);
        await this.disposeCollected(activation);
      }
    }
    for (const [key, list] of this.workerActivations) {
      for (const activation of list) {
        await this.collectOne(activation, ageLimitOverrideMs);
      }
      const collected = list.filter((a) => a.state === "invalid");
      const remaining = list.filter((a) => a.state !== "invalid");
      if (remaining.length === 0) this.workerActivations.delete(key);
      else this.workerActivations.set(key, remaining);
      for (const activation of collected) await this.disposeCollected(activation);
    }
  }

  async deactivateAll(reason: DeactivationReason): Promise<void> {
    const all = [...this.activations.values(), ...[...this.workerActivations.values()].flat()];
    await Promise.all(all.map((a) => a.deactivate(reason)));
    this.activations.clear();
    this.workerActivations.clear();
    for (const a of all) {
      if (this.options.grainActivator?.disposeInstance !== undefined) {
        await this.options.grainActivator.disposeInstance(a.instance, a.id);
      }
      this.options.deactivateState?.(a.instance, a.id);
      this.options.onDeactivated?.(a);
    }
  }
}
