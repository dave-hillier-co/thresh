import * as os from "node:os";
import { setupDepth } from "@thresh/core/define-grain";
import { GrainCallError, RejectionError } from "@thresh/core/errors";
import type { Grain } from "@thresh/core/grain";
import type { GrainClass } from "@thresh/core/grain-class";
import type { IncomingGrainCallFilter } from "@thresh/core/grain-call-filter";
import type { BroadcastChannelProvider } from "@thresh/core/broadcast-channel";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { DependencyToken } from "@thresh/core/grain-dependency";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainKeyKind } from "@thresh/core/grain-key";
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
import { constructGrain } from "@thresh/runtime/construct-grain";
import { GrainRuntimeImpl } from "@thresh/runtime/grain-runtime-impl";
import type { GrainServiceRegistry } from "@thresh/runtime/grain-service";
import type { SiloLoadSheddingTestHooks } from "@thresh/runtime/load-shedding";
import type { TimeProvider } from "@thresh/runtime/time-provider";

export interface RegisteredGrain {
  ctor: GrainClass;
  metadata: GrainMetadata;
  /** The key kind the grain type declares, when it declares one as a value. */
  keyKind?: GrainKeyKind;
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
 * How many deactivations one idle sweep runs at once — Orleans'
 * `DeactivateActivationsFromCollector` bound,
 * `MaxDegreeOfParallelism = Environment.ProcessorCount * 512`.
 */
const COLLECTION_CONCURRENCY = (os.cpus().length || 4) * 512;

/**
 * Optional hook to construct/dispose grain instances instead of `new ctor()`
 * (Orleans `IGrainActivator`) — e.g. object pooling or non-DI construction.
 */
export interface GrainActivator {
  /**
   * Construct a grain instance instead of `new ctor()`. `ctor` is the class as
   * it was registered (`GrainClass`), so an activator can hand a grain whose
   * constructor takes an options bag its bag; `constructGrain(ctor)` is the
   * default `new ctor()` path, for a fall-through branch.
   */
  createInstance(ctor: GrainClass, id: GrainId): Grain;
  /** Called when an activation using this activator is deactivated (idle collection or explicit). */
  disposeInstance?(instance: Grain, id: GrainId): void | Promise<void>;
  /**
   * Whether this activator builds `ctor`. Several activators can be registered, each for the
   * classes it claims (see {@link composeGrainActivators}); one that omits `claims` claims every
   * class, so the single-activator form is unchanged.
   */
  claims?(ctor: GrainClass): boolean;
}

/**
 * One activator out of several. A class goes to the first activator that claims it, in
 * registration order; one claimed by none is constructed with `constructGrain`. Disposal goes back
 * to whichever activator built the instance.
 */
export function composeGrainActivators(activators: readonly GrainActivator[]): GrainActivator {
  const builtBy = new WeakMap<Grain, GrainActivator>();
  return {
    createInstance(ctor, id) {
      const activator = activators.find((a) => a.claims === undefined || a.claims(ctor));
      if (activator === undefined) return constructGrain(ctor);
      const instance = activator.createInstance(ctor, id);
      builtBy.set(instance, activator);
      return instance;
    },
    async disposeInstance(instance, id) {
      await builtBy.get(instance)?.disposeInstance?.(instance, id);
    },
  };
}

export interface CatalogOptions {
  grainTypes: ReadonlyMap<GrainType, RegisteredGrain>;
  factory: GrainFactory;
  time: TimeProvider;
  defaultCollectionAgeSeconds: number;
  /**
   * Per-silo idle-deactivation ages keyed by GRAIN TYPE (Orleans'
   * `GrainCollectionOptions.ClassSpecificCollectionAge`, which Orleans keys by
   * grain class name and seeds from `[CollectionAgeLimit]`). An entry OVERRIDES
   * the grain type's own `@grain({ collectionAgeSeconds })`, which stays the
   * default; a grain type with no entry is unaffected, so an absent map leaves
   * the decorator in sole charge. Keyed by grain type rather than `class.name`
   * because a bundler may rename the class but never the registered type.
   */
  classSpecificCollectionAgeSeconds?: Readonly<Record<GrainType, number>>;
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
  /**
   * The migration hand-off of a deactivation pipeline: dehydrate `activation`
   * and move it to `target`, or to a placement-chosen silo when `target` is
   * undefined; resolves to whether it moved (see `DeactivationSteps.migrate`).
   */
  migrate?: (activation: ActivationData, target: SiloAddress | undefined) => Promise<boolean>;
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
  /** This silo's per-silo grain dependencies, for a functional grain's `useDependency`. */
  grainDependencies?: ReadonlyMap<DependencyToken<unknown>, unknown>;
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
  /**
   * Set once `deactivateAll` starts (silo shutdown) — Orleans only creates
   * new activations while `SiloStatus.Active` (`Catalog.cs` ~150); once this
   * silo is stopping, `getOrActivate` refuses to create rather than handing
   * out an activation `deactivateAll` has already swept past, which would
   * otherwise never be deactivated or unregistered (issue #108).
   */
  private stopping = false;
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
   * directory CAS, holding the entry its registration returned (`registration`).
   * Returns an existing live activation if one is already here. The new activation
   * is stored before this returns, so a directory recovery answered from here on
   * reports it.
   */
  activateLocal(
    id: GrainId,
    activationId: string,
    registration?: GrainAddress,
  ): Promise<ActivationData> {
    return this.getOrActivate(id, activationId, undefined, undefined, registration);
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
   * create, store, and return one. A deactivating activation still counts as
   * present (Orleans hands a message to the activation its catalog holds): a
   * call to it is held and rerouted once its pipeline has removed it, and the
   * reroute's lookup creates the replacement.
   *
   * Deliberately NOT declared `async`: the check-then-set on
   * `this.activations` must run to completion in one synchronous slice,
   * uninterrupted by a microtask hop. `async function` bodies yield at their
   * first `await` even when awaiting an already-resolved value, which would
   * let two calls for the same not-yet-created id both pass the "doesn't
   * exist" check before either created + stored one — Promise.all([
   * g.increment(1), g.increment(2), ...]) would then activate the SAME grain
   * id multiple times, splitting the calls across separate activations
   * instead of serializing them onto one.
   */
  private getOrActivate(
    id: GrainId,
    activationId?: string,
    rehydrationBag?: Record<string, unknown>,
    sourceAddr?: GrainAddress,
    registration?: GrainAddress,
  ): Promise<ActivationData> {
    id = this.canonicalId(id);
    const key = id.toString();
    const existing = this.activations.get(key);
    if (existing !== undefined && existing.state !== "invalid") return Promise.resolve(existing);
    return this.createAndStore(key, id, activationId, rehydrationBag, sourceAddr, registration);
  }

  /**
   * Create a fresh activation and store it — refusing while this silo is
   * stopping (see `stopping`) instead of handing out one `deactivateAll` may
   * never see and deactivate.
   */
  private createAndStore(
    key: string,
    id: GrainId,
    activationId?: string,
    rehydrationBag?: Record<string, unknown>,
    sourceAddr?: GrainAddress,
    registration?: GrainAddress,
  ): Promise<ActivationData> {
    if (this.stopping) {
      return Promise.reject(
        new RejectionError(`silo stopping: cannot activate ${id.toString()}`, "siloDraining"),
      );
    }
    const created = this.create(id, activationId, rehydrationBag, sourceAddr);
    created.registration = registration;
    this.activations.set(key, created);
    return Promise.resolve(created);
  }

  /**
   * The deactivation pipeline's removal step (`DeactivationSteps.remove`,
   * Orleans `UnregisterMessageTarget`): drop `activation` from whichever map
   * holds it (ordinary or stateless-worker) — only if it is still the one
   * stored, so a fresh activation that replaced it under the same key while
   * its hook ran is never deleted (issue #106).
   */
  private remove(activation: ActivationData): void {
    const key = activation.id.toString();
    if (this.activations.get(key) === activation) {
      this.activations.delete(key);
    } else {
      const list = this.workerActivations.get(key);
      if (list !== undefined) {
        const remaining = list.filter((a) => a !== activation);
        if (remaining.length === 0) this.workerActivations.delete(key);
        else this.workerActivations.set(key, remaining);
      }
    }
  }

  /** The deactivation pipeline's disposal step: dispose the instance, then unbind its state. */
  private async dispose(activation: ActivationData): Promise<void> {
    if (this.options.grainActivator?.disposeInstance !== undefined) {
      await this.options.grainActivator.disposeInstance(activation.instance, activation.id);
    }
    this.options.deactivateState?.(activation.instance, activation.id);
  }

  /**
   * Return the live (non-invalid) activation for `id` if one exists, WITHOUT
   * creating one — called directly by `DistributedDispatcher`
   * (`routeTo`/`deliverLocal`), which resolve an already-cached/directory-
   * known address straight to the catalog and invoke it, bypassing
   * `getOrCreate`/`activateLocal` entirely once an activation exists. A
   * deactivating activation is returned like any other: it holds the call
   * and reroutes it once it has gone.
   */
  resolveLive(id: GrainId): Promise<ActivationData | undefined> {
    const key = this.canonicalId(id).toString();
    const existing = this.activations.get(key);
    if (existing === undefined || existing.state === "invalid") return Promise.resolve(undefined);
    return Promise.resolve(existing);
  }

  /** A live activation for `id` — for a stateless-worker id, an arbitrary one of possibly several. */
  get(id: GrainId): ActivationData | undefined {
    const key = this.canonicalId(id).toString();
    const single = this.activations.get(key);
    if (single !== undefined) return single;
    return this.workerActivations.get(key)?.find((a) => a.state !== "invalid");
  }

  isActive(id: GrainId): boolean {
    const key = this.canonicalId(id).toString();
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
    const key = this.canonicalId(id).toString();
    const single = this.activations.get(key);
    if (single !== undefined) return single.state === "invalid" ? 0 : 1;
    const list = this.workerActivations.get(key);
    if (list === undefined) return 0;
    let n = 0;
    for (const a of list) if (a.state !== "invalid") n++;
    return n;
  }

  /**
   * The id in the one canonical text its grain type reads: a key arriving as text is re-read as
   * the kind the type declares, so `'005'`, `'+5'` and `5n` are one identity for an integer grain
   * and an upper-case guid string is the Guid it spells. Every lookup, the directory and the ring
   * go through this, so equality, hash and catalog key agree. Text the declared kind cannot read
   * is left as it is: it names no activation, and creating one reports the error.
   */
  canonicalId(id: GrainId): GrainId {
    const kind = this.options.grainTypes.get(id.type)?.keyKind;
    if (kind === undefined) return id;
    try {
      return id.asKind(kind);
    } catch {
      return id;
    }
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

  /**
   * The activations that stand for a directory entry: registered (holding the address their
   * registration returned) and not deactivating or finished — creating, activating or valid
   * (Orleans `GetRegisteredActivations` skips unregistered, deactivating and invalid ones).
   * Stateless workers are never registered.
   */
  registeredActivations(): ActivationData[] {
    const out: ActivationData[] = [];
    for (const a of this.activations.values()) {
      if (a.registration === undefined) continue;
      if (a.state === "creating" || a.state === "activating" || a.state === "valid") out.push(a);
    }
    return out;
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
   * The `[StatelessWorker]` path on a silo that creates no activation (one outside its ring, see
   * `DistributedDispatcherDeps.mayActivate`): one of `id`'s existing local workers that can serve
   * a call — a non-busy one first, else the least loaded — or `undefined` when it hosts none.
   * Never creates a worker. Synchronous, for the reason `pickOrScaleWorker` gives.
   */
  pickExistingWorker(id: GrainId): ActivationData | undefined {
    const list = this.workerActivations.get(this.canonicalId(id).toString());
    if (list === undefined) return undefined;
    let best: ActivationData | undefined;
    for (const a of list) {
      if (a.state === "invalid" || a.state === "deactivating" || a.state === "failed") continue;
      if (!a.scheduler.busy) return a;
      if (best === undefined || a.scheduler.load < best.scheduler.load) best = a;
    }
    return best;
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
    id = this.canonicalId(id);
    const key = id.toString();
    let list = this.workerActivations.get(key);
    if (list === undefined) {
      list = [];
      this.workerActivations.set(key, list);
    }
    // Belt and braces: the deactivation pipeline's removal step already takes
    // a finished worker out of this list.
    for (let i = list.length - 1; i >= 0; i--) {
      if (list[i]!.state === "invalid") list.splice(i, 1);
    }
    // A deactivating worker would only hold the call and reroute it.
    for (const a of list) {
      if (a.state !== "deactivating" && a.state !== "failed" && !a.scheduler.busy) return a;
    }
    if (list.length < maxLocalWorkers) {
      // Same gate as `createAndStore`: a worker created once `deactivateAll`
      // has snapshotted would be dropped by its `clear()` without its
      // `onDeactivate` ever running.
      if (this.stopping) {
        throw new RejectionError(`silo stopping: cannot activate ${id.toString()}`, "siloDraining");
      }
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
    // No grain factory may be running when a new activation starts: the
    // functional-grain hooks resolve against a stack that `setContext` pushes and
    // pops synchronously, so a non-empty stack here means a setup leaked and the
    // next grain's facets would be registered against the wrong instance.
    if (setupDepth() !== 0) {
      throw new Error(
        `grain factory setup leaked: ${setupDepth()} active setup(s) when creating ${id.toString()}`,
      );
    }
    const reg = this.options.grainTypes.get(id.type);
    if (reg === undefined) throw new GrainCallError(`no grain type registered: ${id.type}`);
    // Identity is type and key text; the activation reads that text as the kind the grain type
    // declares, whichever typed or string-built id arrived first.
    if (reg.keyKind !== undefined) id = id.asKind(reg.keyKind);
    // Orleans' order, from `GrainTypeSharedContext.GetCollectionAgeLimit`: the grain class's own
    // declaration first (the `IdleDeactivationPeriod` property written by `[CollectionAgeLimit]`,
    // which `@grain({ collectionAgeSeconds })` stands in for), THEN the per-silo class-specific
    // map, then the silo default. A grain type that wants per-silo control declares no age of its
    // own - the decorator is the stronger statement, not the weaker one.
    const ageSeconds =
      reg.metadata.options.collectionAgeSeconds ??
      this.options.classSpecificCollectionAgeSeconds?.[id.type] ??
      this.options.defaultCollectionAgeSeconds;
    const activation = new ActivationData(
      id,
      this.options.time,
      ageSeconds * 1000,
      reg.metadata.reentrant,
      activationId,
      {
        ...this.options.activationOptions,
        deactivationSteps: {
          ...(this.options.migrate !== undefined ? { migrate: this.options.migrate } : {}),
          unregister: (a) => this.options.onDeactivated?.(a),
          remove: (a) => this.remove(a),
          dispose: (a) => this.dispose(a),
        },
      },
    );
    activation.runtime = new GrainRuntimeImpl(this.options.factory, activation, {
      time: this.options.time,
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
      ...(this.options.grainDependencies !== undefined
        ? { dependencies: this.options.grainDependencies }
        : {}),
      isStatelessWorker: () => reg.metadata.options.stateless === true,
    });
    // Load-bearing: nothing may `await` between creating the instance and binding
    // its context. `setContext` runs a functional grain's factory, and the hooks
    // it calls resolve against a module-level setup stack in `@thresh/core`; an
    // await here would let another activation interleave and steal that setup.
    const instance =
      this.options.grainActivator !== undefined
        ? this.options.grainActivator.createInstance(reg.ctor, id)
        : constructGrain(reg.ctor);
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
   * If still stale, deactivate it through its pipeline, which removes,
   * unregisters and disposes it once finished. `ageLimitOverrideMs`, when
   * given, is `ForceActivationCollection`'s caller-chosen age instead of each
   * activation's own collection age (see `ActivationData.isStale`). Only
   * age decides staleness: an explicit deactivation or migration request has
   * already moved its activation to deactivating by itself.
   */
  private async collectOne(activation: ActivationData, ageLimitOverrideMs?: number): Promise<void> {
    if (!activation.isStale(ageLimitOverrideMs)) return;
    await activation.deactivate({ code: "idle", description: "idle collection" });
  }

  /** Every activation — ordinary and stateless-worker alike — that is stale now, in one pass. */
  private selectStale(ageLimitOverrideMs?: number): ActivationData[] {
    const stale: ActivationData[] = [];
    for (const activation of this.activations.values()) {
      if (activation.isStale(ageLimitOverrideMs)) stale.push(activation);
    }
    for (const list of this.workerActivations.values()) {
      for (const activation of list) {
        if (activation.isStale(ageLimitOverrideMs)) stale.push(activation);
      }
    }
    return stale;
  }

  /**
   * Sweep every activation — ordinary and stateless-worker alike — and
   * deactivate the stale ones. `ageLimitOverrideMs` is
   * `IManagementGrain.forceActivationCollection`'s forced sweep age (e.g. `0`
   * to collect every currently-idle activation); omitted, each activation
   * uses its own configured collection age (the periodic `ActivationCollector`).
   */
  async collectIdle(ageLimitOverrideMs?: number): Promise<void> {
    // Selected in one pass, then deactivated through each activation's own
    // pipeline with bounded concurrency (Orleans `Parallel.ForEachAsync` in
    // `DeactivateActivationsFromCollector`), so a sweep's throughput is not
    // one deactivation per hook latency. `collectOne` never rejects.
    const stale = this.selectStale(ageLimitOverrideMs);
    let next = 0;
    const drain = async (): Promise<void> => {
      while (next < stale.length) {
        const activation = stale[next++]!;
        await this.collectOne(activation, ageLimitOverrideMs);
      }
    };
    const workers = Math.min(COLLECTION_CONCURRENCY, stale.length);
    await Promise.all(Array.from({ length: workers }, drain));
  }

  /**
   * `deadlineMs`, when given, bounds the WHOLE sweep (Orleans cancels
   * `DeactivateAllActivations(ct)` with the host's own stop token) — separate
   * from each activation's own per-hook `deactivationTimeoutMs`, which bounds
   * only one activation's `onDeactivate` call. Without an overall deadline, a
   * grace period plus N activations each individually capped at, say, 30s can
   * still add up (in the pathological case of many slow hooks queued behind
   * each other on a saturated scheduler) to longer than the process's own
   * termination grace period, so the whole silo gets SIGKILLed mid-stop
   * instead of finishing cleanly (issue #108). An activation still mid-flight
   * when the deadline passes is left alone rather than force-finalized: its
   * hook keeps running in the background (same trade-off as
   * `awaitWithDeactivationTimeout`), and it is skipped below rather than
   * disposed/unregistered out from under itself.
   */
  async deactivateAll(reason: DeactivationReason, deadlineMs?: number): Promise<void> {
    // Set BEFORE snapshotting: `getOrActivate` is synchronous up to its first
    // await, so this is visible to every subsequent call — including one
    // already racing this one — before it can create and store an activation
    // this sweep's snapshot has already missed.
    this.stopping = true;
    const all = [...this.activations.values(), ...[...this.workerActivations.values()].flat()];
    // Each joins a deactivation already under way (an idle sweep, a
    // migration) rather than finalizing under its running hook.
    const deactivated = Promise.all(all.map((a) => a.deactivate(reason)));
    if (deadlineMs === undefined) {
      await deactivated;
    } else {
      let handle: ReturnType<TimeProvider["setTimer"]> | undefined;
      const timedOut = new Promise<void>((resolve) => {
        handle = this.options.time.setTimer(resolve, deadlineMs);
      });
      await Promise.race([deactivated, timedOut]);
      if (handle !== undefined) this.options.time.clearTimer(handle);
    }
    // Every finished activation removed itself; one the deadline cut off is
    // dropped here and finishes (unregister, disposal) on its own once its
    // hook returns — never under it.
    this.activations.clear();
    this.workerActivations.clear();
  }

  /**
   * Drop every activation without deactivating it, for a silo that has fenced itself
   * (`ClusterNode.fence`, Orleans `KillMyselfLocally`): each is abandoned
   * (`ActivationData.abandonFenced`) — no hook, no unregistration, no disposal — and none is
   * created from here on, as during `deactivateAll`.
   */
  abandonAll(): void {
    this.stopping = true;
    const all = [...this.activations.values(), ...[...this.workerActivations.values()].flat()];
    this.activations.clear();
    this.workerActivations.clear();
    for (const activation of all) activation.abandonFenced();
  }
}
