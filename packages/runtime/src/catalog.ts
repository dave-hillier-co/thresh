import { GrainCallError } from "@tsva/core/errors";
import type { Grain } from "@tsva/core/grain";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainMetadata } from "@tsva/core/grain-metadata";
import type { GrainType } from "@tsva/core/grain-type";
import type { DeactivationReason } from "@tsva/core/reasons";
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
}

/** Registry of live activations on this silo, keyed by grain id. */
export class Catalog {
  private readonly activations = new Map<string, ActivationData>();

  constructor(private readonly options: CatalogOptions) {}

  getOrCreate(id: GrainId): ActivationData {
    const existing = this.activations.get(id.toString());
    if (existing !== undefined && existing.state !== "invalid") return existing;
    const created = this.create(id);
    this.activations.set(id.toString(), created);
    return created;
  }

  get(id: GrainId): ActivationData | undefined {
    return this.activations.get(id.toString());
  }

  isActive(id: GrainId): boolean {
    return this.activations.get(id.toString())?.state === "valid";
  }

  private create(id: GrainId): ActivationData {
    const reg = this.options.grainTypes.get(id.type);
    if (reg === undefined) throw new GrainCallError(`no grain type registered: ${id.type}`);
    const ageSeconds =
      reg.metadata.options.collectionAgeSeconds ?? this.options.defaultCollectionAgeSeconds;
    const activation = new ActivationData(
      id,
      this.options.time,
      ageSeconds * 1000,
      reg.metadata.reentrant,
    );
    activation.runtime = new GrainRuntimeImpl(this.options.factory, activation);
    const instance = new reg.ctor();
    instance.setContext(activation);
    activation.instance = instance;
    activation.beginActivate("incoming-call");
    return activation;
  }

  async collectIdle(): Promise<void> {
    for (const [key, activation] of this.activations) {
      if (activation.isStale()) {
        await activation.deactivate({ code: "idle", description: "idle collection" });
      }
      if (activation.state === "invalid") this.activations.delete(key);
    }
  }

  async deactivateAll(reason: DeactivationReason): Promise<void> {
    await Promise.all([...this.activations.values()].map((a) => a.deactivate(reason)));
    this.activations.clear();
  }
}
