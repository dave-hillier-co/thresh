import type { GrainClass } from "@thresh/core/grain-class";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import {
  type GrainRegistration,
  normalizeRegistration,
  type Registrable,
} from "@thresh/core/grain-registration";
import type { GrainType } from "@thresh/core/grain-type";
import type { KeyTypeOf } from "@thresh/core/key-kinds";
import { ActivationCollector } from "@thresh/runtime/activation-collector";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { LocalDispatcher } from "@thresh/runtime/local-dispatcher";
import { systemTimeProvider, type TimeProvider } from "@thresh/runtime/time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";

export interface SiloOptions {
  time?: TimeProvider;
  defaultCollectionAgeSeconds?: number;
  collectionIntervalSeconds?: number;
}

/** Re-exported so hosts that register against a `Silo` need only one import. */
export type { GrainRegistration };

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

  /**
   * Register a `defineGrain`/`defineReducerGrain` definition. It carries its own
   * interface, so `extra` is only needed to name the interfaces it answers to
   * instead — an explicit list wins outright and the fused interface is not
   * added to it.
   */
  registerGrain(definition: Registrable, extra?: GrainRegistration): this;
  /** Register a class grain under the interfaces it serves. */
  registerGrain(ctor: GrainClass, registration: GrainRegistration): this;
  registerGrain(definition: Registrable | GrainClass, registration?: GrainRegistration): this {
    const { ctor, interfaces } = normalizeRegistration(definition, registration);
    const metadata = getGrainMetadata(ctor);
    if (metadata === undefined) throw new Error(`${ctor.name} is not decorated with @grain()`);
    this.grainTypes.set(metadata.grainType, { ctor, metadata });
    for (const iface of interfaces) {
      this.interfaceToGrainType.set(iface.id, metadata.grainType);
    }
    return this;
  }

  getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: KeyTypeOf<K>): T {
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
