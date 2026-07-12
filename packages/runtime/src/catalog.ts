import { GrainCallError } from "@tsva/core/errors";
import type { Grain } from "@tsva/core/grain";
import type { IncomingGrainCallFilter } from "@tsva/core/grain-call-filter";
import type { BroadcastChannelProvider } from "@tsva/core/broadcast-channel";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainMetadata } from "@tsva/core/grain-metadata";
import type { GrainType } from "@tsva/core/grain-type";
import type { DeactivationReason } from "@tsva/core/reasons";
import type { ReminderRegistry } from "@tsva/core/reminder";
import type { DurableJobScheduler } from "@tsva/core/durable-job";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { StreamProvider } from "@tsva/core/stream";
import { ActivationData } from "@tsva/runtime/activation";
import {
  cancellationExtensionFactory,
  ICancellationSourcesExtension,
} from "@tsva/runtime/cancellation-extension";
import type { GrainFactory } from "@tsva/runtime/grain-factory";
import {
  grainManagementExtensionFactory,
  IGrainManagementExtension,
} from "@tsva/runtime/grain-management-extension";
import { GrainRuntimeImpl } from "@tsva/runtime/grain-runtime-impl";
import type { TimeProvider } from "@tsva/runtime/time-provider";

export interface RegisteredGrain {
  ctor: new () => Grain;
  metadata: GrainMetadata;
}

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
  /** Incoming call filters wrapping each grain-method dispatch (silo-wide). */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
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
}

/** Registry of live activations on this silo, keyed by grain id. */
export class Catalog {
  private readonly activations = new Map<string, ActivationData>();
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
  ): Promise<ActivationData> {
    return this.getOrActivate(id, activationId, bag);
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
  ): Promise<ActivationData> {
    const key = id.toString();
    const existing = this.activations.get(key);
    if (existing !== undefined && existing.state !== "invalid") {
      if (!existing.deactivationRequestedAndIdle) return Promise.resolve(existing);
      return this.finalizeStale(key, existing).then(() => {
        const created = this.create(id, activationId, rehydrationBag);
        this.activations.set(key, created);
        return created;
      });
    }
    const created = this.create(id, activationId, rehydrationBag);
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

  get(id: GrainId): ActivationData | undefined {
    return this.activations.get(id.toString());
  }

  isActive(id: GrainId): boolean {
    return this.activations.get(id.toString())?.state === "valid";
  }

  /** Live activation count, used by activation-count placement. */
  count(): number {
    let n = 0;
    for (const a of this.activations.values()) if (a.state !== "invalid") n++;
    return n;
  }

  /** Valid (servable) activations — migration candidates for the rebalancer. */
  liveActivations(): ActivationData[] {
    const out: ActivationData[] = [];
    for (const a of this.activations.values()) if (a.state === "valid") out.push(a);
    return out;
  }

  private create(
    id: GrainId,
    activationId?: string,
    rehydrationBag?: Record<string, unknown>,
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
    activation.setExtensionFactories(this.extensionFactories);
    const activateState = this.options.activateState;
    if (rehydrationBag !== undefined) {
      activation.rehydrationBag = rehydrationBag;
      activation.preActivate = async () => {
        if (activateState !== undefined) await activateState(instance, id, "rehydrate");
        activation.applyRehydration();
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

  async collectIdle(): Promise<void> {
    for (const [key, activation] of this.activations) {
      if (activation.isStale()) {
        if (activation.wantsMigration && this.options.migrate !== undefined) {
          // Migration was requested before this sweep: dehydrate the grain's
          // state and hand it off first (so `onDeactivate` — which may clear
          // state — runs only after the state has been captured), then run the
          // deactivate hook with the "migrating" reason.
          const moved = await this.options.migrate(activation);
          await activation.deactivate(
            moved
              ? { code: "migrating", description: "migrated to another silo" }
              : { code: "idle", description: "idle collection" },
          );
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
      if (activation.state === "invalid") {
        this.activations.delete(key);
        if (this.options.grainActivator?.disposeInstance !== undefined) {
          await this.options.grainActivator.disposeInstance(activation.instance, activation.id);
        }
        this.options.onDeactivated?.(activation);
      }
    }
  }

  async deactivateAll(reason: DeactivationReason): Promise<void> {
    const all = [...this.activations.values()];
    await Promise.all(all.map((a) => a.deactivate(reason)));
    this.activations.clear();
    for (const a of all) {
      if (this.options.grainActivator?.disposeInstance !== undefined) {
        await this.options.grainActivator.disposeInstance(a.instance, a.id);
      }
      this.options.onDeactivated?.(a);
    }
  }
}
