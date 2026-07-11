import type { Grain } from "@tsva/core/grain";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import type { GrainType } from "@tsva/core/grain-type";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import { ActivationCollector } from "@tsva/runtime/activation-collector";
import { Catalog, type RegisteredGrain } from "@tsva/runtime/catalog";
import { GrainFactory } from "@tsva/runtime/grain-factory";
import { LocalDispatcher } from "@tsva/runtime/local-dispatcher";
import { systemTimeProvider, type TimeProvider } from "@tsva/runtime/time-provider";
import { TransactionAgent } from "@tsva/runtime/transaction-agent";

export interface SiloOptions {
  time?: TimeProvider;
  defaultCollectionAgeSeconds?: number;
  collectionIntervalSeconds?: number;
}

export interface GrainRegistration {
  /** The grain interfaces this implementation serves, used to route `getGrain`. */
  interfaces: GrainInterface<unknown>[];
}

/** A single-process silo (Phase 1): catalog, scheduler, factory, collector. */
export class Silo {
  private readonly grainTypes = new Map<GrainType, RegisteredGrain>();
  private readonly interfaceToGrainType = new Map<number, GrainType>();
  private readonly factory: GrainFactory;
  private readonly catalog: Catalog;
  private readonly collector: ActivationCollector;

  constructor(options: SiloOptions = {}) {
    const time = options.time ?? systemTimeProvider;
    this.factory = new GrainFactory((interfaceId) => this.resolveGrainType(interfaceId), time);
    this.catalog = new Catalog({
      grainTypes: this.grainTypes,
      factory: this.factory,
      time,
      defaultCollectionAgeSeconds: options.defaultCollectionAgeSeconds ?? 900,
    });
    const dispatcher = new LocalDispatcher(this.catalog);
    this.factory.setDispatcher(dispatcher);
    const agent = new TransactionAgent(time);
    agent.setDispatcher(dispatcher);
    this.factory.setTransactionAgent(agent);
    this.collector = new ActivationCollector(
      this.catalog,
      time,
      (options.collectionIntervalSeconds ?? 60) * 1000,
    );
  }

  registerGrain<G extends Grain>(ctor: new () => G, registration: GrainRegistration): this {
    const metadata = getGrainMetadata(ctor);
    if (metadata === undefined) throw new Error(`${ctor.name} is not decorated with @grain()`);
    this.grainTypes.set(metadata.grainType, { ctor, metadata });
    for (const iface of registration.interfaces) {
      this.interfaceToGrainType.set(iface.id, metadata.grainType);
    }
    return this;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.factory.getGrain(def, key);
  }

  isActive(id: GrainId): boolean {
    return this.catalog.isActive(id);
  }

  start(): void {
    this.collector.start();
  }

  async stop(): Promise<void> {
    this.collector.stop();
    await this.catalog.deactivateAll({ code: "shutting-down", description: "silo stopping" });
  }

  private resolveGrainType(interfaceId: number): GrainType {
    const grainType = this.interfaceToGrainType.get(interfaceId);
    if (grainType === undefined) {
      throw new Error(`no grain registered for interface ${interfaceId}`);
    }
    return grainType;
  }
}
