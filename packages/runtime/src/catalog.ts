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
import type { StreamProvider } from "@tsva/core/stream";
import { ActivationData } from "@tsva/runtime/activation";
import type { GrainFactory } from "@tsva/runtime/grain-factory";
import { GrainRuntimeImpl } from "@tsva/runtime/grain-runtime-impl";
import type { TimeProvider } from "@tsva/runtime/time-provider";

export interface RegisteredGrain {
  ctor: new () => Grain;
  metadata: GrainMetadata;
}

export interface CatalogOptions {
  grainTypes: ReadonlyMap<GrainType, RegisteredGrain>;
  factory: GrainFactory;
  time: TimeProvider;
  defaultCollectionAgeSeconds: number;
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
  /** Incoming call filters wrapping each grain-method dispatch (silo-wide). */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
}

/** Registry of live activations on this silo, keyed by grain id. */
export class Catalog {
  private readonly activations = new Map<string, ActivationData>();

  constructor(private readonly options: CatalogOptions) {}

  /** Single-silo path (Phase 1): create with a fresh activation id if absent. */
  getOrCreate(id: GrainId): ActivationData {
    return this.getOrActivate(id);
  }

  /**
   * Multi-silo path: activate with the activation id the dispatcher won via
   * directory CAS. Returns an existing live activation if one is already here.
   */
  activateLocal(id: GrainId, activationId: string): ActivationData {
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
  ): ActivationData {
    return this.getOrActivate(id, activationId, bag);
  }

  /** Lookup-create-store: return a live activation if present, otherwise create, store, and return one. */
  private getOrActivate(
    id: GrainId,
    activationId?: string,
    rehydrationBag?: Record<string, unknown>,
  ): ActivationData {
    const existing = this.activations.get(id.toString());
    if (existing !== undefined && existing.state !== "invalid") return existing;
    const created = this.create(id, activationId, rehydrationBag);
    this.activations.set(id.toString(), created);
    return created;
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
    });
    const instance = new reg.ctor();
    instance.setContext(activation);
    activation.instance = instance;
    if (this.options.incomingCallFilters !== undefined) {
      activation.incomingCallFilters = this.options.incomingCallFilters;
    }
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
        // A grain that asked to migrate moves to another silo first (flipping the
        // directory); then the local activation is deactivated either way — its
        // address-matched directory unregister is a no-op once the entry moved.
        const moved =
          activation.wantsMigration && this.options.migrate !== undefined
            ? await this.options.migrate(activation)
            : false;
        await activation.deactivate(
          moved
            ? { code: "migrating", description: "migrated to another silo" }
            : { code: "idle", description: "idle collection" },
        );
      }
      if (activation.state === "invalid") {
        this.activations.delete(key);
        this.options.onDeactivated?.(activation);
      }
    }
  }

  async deactivateAll(reason: DeactivationReason): Promise<void> {
    const all = [...this.activations.values()];
    await Promise.all(all.map((a) => a.deactivate(reason)));
    this.activations.clear();
    for (const a of all) this.options.onDeactivated?.(a);
  }
}
