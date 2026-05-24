import { GrainCallError } from "@tsva/core/errors";
import type { Grain } from "@tsva/core/grain";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainMetadata } from "@tsva/core/grain-metadata";
import type { GrainType } from "@tsva/core/grain-type";
import type { DeactivationReason } from "@tsva/core/reasons";
import type { ReminderRegistry } from "@tsva/core/reminder";
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
  /** Bind/read persistent state before `onActivate` (provided by the hosting layer). */
  activateState?: (instance: Grain, grainId: GrainId) => Promise<void>;
  /** Resolves the reminder registry a grain's `registerReminder` delegates to. */
  reminderRegistry?: () => ReminderRegistry | undefined;
  /** Resolves the stream provider a grain's `getStreamProvider` returns. */
  streamProvider?: (name?: string) => StreamProvider | undefined;
}

/** Registry of live activations on this silo, keyed by grain id. */
export class Catalog {
  private readonly activations = new Map<string, ActivationData>();

  constructor(private readonly options: CatalogOptions) {}

  /** Single-silo path (Phase 1): create with a fresh activation id if absent. */
  getOrCreate(id: GrainId): ActivationData {
    const existing = this.activations.get(id.toString());
    if (existing !== undefined && existing.state !== "invalid") return existing;
    const created = this.create(id);
    this.activations.set(id.toString(), created);
    return created;
  }

  /**
   * Multi-silo path: activate with the activation id the dispatcher won via
   * directory CAS. Returns an existing live activation if one is already here.
   */
  activateLocal(id: GrainId, activationId: string): ActivationData {
    const existing = this.activations.get(id.toString());
    if (existing !== undefined && existing.state !== "invalid") return existing;
    const created = this.create(id, activationId);
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

  private create(id: GrainId, activationId?: string): ActivationData {
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
    });
    const instance = new reg.ctor();
    instance.setContext(activation);
    activation.instance = instance;
    const activateState = this.options.activateState;
    if (activateState !== undefined) {
      activation.preActivate = () => activateState(instance, id);
    }
    activation.beginActivate("incoming-call");
    return activation;
  }

  async collectIdle(): Promise<void> {
    for (const [key, activation] of this.activations) {
      if (activation.isStale()) {
        await activation.deactivate({ code: "idle", description: "idle collection" });
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
