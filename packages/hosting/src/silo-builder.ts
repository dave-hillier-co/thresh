import type { Grain } from "@tsva/core/grain";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { MembershipService } from "@tsva/core/membership";
import type { SiloAddress } from "@tsva/core/silo-address";
import {
  KubernetesMembership,
  type EndpointWatch,
  type KubernetesMembershipOptions,
} from "@tsva/clustering-k8s/kubernetes-membership";
import type { GrainStorage } from "@tsva/core/grain-storage";
import { InProcessTransport, type InProcessNetwork } from "@tsva/messaging/in-process-transport";
import type { Transport } from "@tsva/messaging/transport";
import { WebSocketTransport } from "@tsva/messaging/web-socket-transport";
import type { ReminderTable } from "@tsva/core/reminder";
import { systemTimeProvider, type TimeProvider } from "@tsva/core/time-provider";
import { MemoryGrainStorage } from "@tsva/persistence/memory-grain-storage";
import { bindPersistentStates } from "@tsva/persistence/state-activator";
import { StorageRegistry } from "@tsva/persistence/storage-registry";
import { LocalReminderService } from "@tsva/reminders/local-reminder-service";
import { MemoryReminderTable } from "@tsva/reminders/memory-reminder-table";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { HealthCheck } from "@tsva/hosting/health-check";
import { HealthServer } from "@tsva/hosting/health-server";
import { buildSiloHost, type SiloHost } from "@tsva/hosting/silo-host";

export interface SiloConfig {
  clusterId: string;
  local: SiloAddress;
  /** Shared clock (defaults to the system clock); inject a fake for tests. */
  time?: TimeProvider;
}

interface Registration {
  ctor: new () => Grain;
  interfaces: GrainInterface<unknown>[];
}

/** Fluent builder for a silo, mirroring the hosting surface in docs/11. */
export class SiloBuilder {
  private membership: MembershipService | undefined;
  private transport: Transport | undefined;
  private healthPort: number | undefined;
  private storage: StorageRegistry | undefined;
  private reminderTable: ReminderTable | undefined;
  private readonly registrations: Registration[] = [];

  constructor(private readonly config: SiloConfig) {}

  /** Enable durable reminders backed by the given table (in-memory by default). */
  useReminders(table: ReminderTable = new MemoryReminderTable()): this {
    this.reminderTable = table;
    return this;
  }

  /** Register a named storage provider (the first becomes "default" if unnamed). */
  addStorage(name: string, provider: GrainStorage): this {
    (this.storage ??= new StorageRegistry()).add(name, provider);
    return this;
  }

  /** Convenience: register an in-memory "default" provider (dev/tests). */
  useMemoryStorage(provider: GrainStorage = new MemoryGrainStorage()): this {
    return this.addStorage("default", provider);
  }

  useStaticMembership(silos: readonly SiloAddress[]): this {
    this.membership = new StaticMembershipService(this.config.local, silos);
    return this;
  }

  useKubernetesMembership(watch: EndpointWatch, options?: KubernetesMembershipOptions): this {
    this.membership = new KubernetesMembership(this.config.local, watch, options);
    return this;
  }

  useInProcessTransport(network: InProcessNetwork): this {
    this.transport = new InProcessTransport(network, this.config.clusterId);
    return this;
  }

  useWebSocketTransport(): this {
    this.transport = new WebSocketTransport(this.config.clusterId);
    return this;
  }

  /** MessagePack is the default serializer; this is here for symmetry with docs/11. */
  useMessagePackSerialization(): this {
    return this;
  }

  useHealthEndpoints(options: { port: number }): this {
    this.healthPort = options.port;
    return this;
  }

  registerGrain<G extends Grain>(
    ctor: new () => G,
    registration: { interfaces: GrainInterface<unknown>[] },
  ): this {
    this.registrations.push({ ctor, interfaces: registration.interfaces });
    return this;
  }

  registerGrains(registrations: Registration[]): this {
    this.registrations.push(...registrations);
    return this;
  }

  build(): SiloHost {
    if (this.membership === undefined) throw new Error("silo: no membership configured");
    if (this.transport === undefined) throw new Error("silo: no transport configured");
    const health = new HealthCheck();
    const storage = this.storage;
    const time = this.config.time;
    let reminderService: LocalReminderService | undefined;

    const node = new ClusterNode({
      local: this.config.local,
      clusterId: this.config.clusterId,
      membership: this.membership,
      transport: this.transport,
      ...(time !== undefined ? { time } : {}),
      ...(storage !== undefined
        ? { stateBinder: (instance, grainId) => bindPersistentStates(instance, grainId, storage) }
        : {}),
      ...(this.reminderTable !== undefined ? { reminderRegistry: () => reminderService } : {}),
    });
    for (const r of this.registrations) node.registerGrain(r.ctor, { interfaces: r.interfaces });

    if (this.reminderTable !== undefined) {
      reminderService = new LocalReminderService(
        this.reminderTable,
        time ?? systemTimeProvider,
        (grainId, name, status) => node.deliverReminder(grainId, name, status),
      );
    }

    return buildSiloHost({
      node,
      health,
      healthServer: this.healthPort !== undefined ? new HealthServer(health) : undefined,
      healthPort: this.healthPort,
      membership: this.membership,
      reminderService,
    });
  }
}

export function createSilo(config: SiloConfig): SiloBuilder {
  return new SiloBuilder(config);
}
