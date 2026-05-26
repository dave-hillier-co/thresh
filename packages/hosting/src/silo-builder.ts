import type { Grain } from "@tsva/core/grain";
import type {
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
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
import { createClient } from "redis";
import { MemoryGrainStorage } from "@tsva/persistence/memory-grain-storage";
import { RedisGrainStorage } from "@tsva/persistence/redis-grain-storage";
import { bindPersistentStates } from "@tsva/persistence/state-activator";
import { bindReducerStates } from "@tsva/persistence/reducer-state-activator";
import { StorageRegistry } from "@tsva/persistence/storage-registry";
import { bindTransactionalStates } from "@tsva/transactions/transactional-state-activator";
import { setupTracePropagation, tracingFilters } from "@tsva/observability/tracing";
import { MemoryTransactionalStorage } from "@tsva/transactions/memory-transactional-storage";
import { RedisTransactionalStorage } from "@tsva/transactions/redis-transactional-storage";
import { TransactionalStorageRegistry } from "@tsva/transactions/transactional-storage-registry";
import type { TransactionalStateStorage } from "@tsva/core/transactional-storage";
import type { StreamProvider } from "@tsva/core/stream";
import { LocalReminderService } from "@tsva/reminders/local-reminder-service";
import { MemoryReminderTable } from "@tsva/reminders/memory-reminder-table";
import { RedisReminderTable } from "@tsva/reminders/redis-reminder-table";
import { MemoryStreamProvider } from "@tsva/streams/memory-stream-provider";
import { RedisPullingStreamProvider } from "@tsva/streams/redis-pulling-stream-provider";
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
  /** Idle-deactivation threshold for grains without their own `collectionAgeSeconds`. */
  collectionAgeSeconds?: number;
  /** How often the idle-collection sweep runs (defaults to 60s). */
  collectionIntervalSeconds?: number;
  /** Injectable RNG for deterministic placement in examples/tests. */
  random?: () => number;
  /** How often each silo re-reads its reminder ranges from the table (defaults to 60s). */
  reminderRefreshSeconds?: number;
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
  private transactionalStorage: TransactionalStorageRegistry | undefined;
  private reminderTable: ReminderTable | undefined;
  private readonly streamProviders = new Map<string, StreamProvider>();
  private readonly incomingCallFilters: IncomingGrainCallFilter[] = [];
  private readonly outgoingCallFilters: OutgoingGrainCallFilter[] = [];
  private readonly registrations: Registration[] = [];
  private readonly starters: Array<() => Promise<void>> = [];
  private readonly closers: Array<() => Promise<void>> = [];
  private readonly pullingStreams: RedisPullingStreamProvider[] = [];

  constructor(private readonly config: SiloConfig) {}

  /** Enable durable reminders backed by the given table (in-memory by default). */
  useReminders(table: ReminderTable = new MemoryReminderTable()): this {
    this.reminderTable = table;
    return this;
  }

  /**
   * Enable durable reminders backed by Redis. The client connects when the silo
   * starts and disconnects when it stops; `keyPrefix` namespaces keys (defaults
   * to `"tsva"`).
   */
  useRedisReminders(options: { url: string; keyPrefix?: string }): this {
    const client = createClient({ url: options.url });
    client.on("error", () => {});
    this.starters.push(async () => {
      await client.connect();
    });
    this.closers.push(async () => {
      await client.close();
    });
    this.reminderTable = new RedisReminderTable(
      client,
      options.keyPrefix !== undefined ? { keyPrefix: options.keyPrefix } : {},
    );
    return this;
  }

  /** Register a stream provider (in-memory by default), named "default" unless given. */
  useMemoryStreams(name = "default"): this {
    this.streamProviders.set(name, new MemoryStreamProvider(name));
    return this;
  }

  addStreamProvider(name: string, provider: StreamProvider): this {
    this.streamProviders.set(name, provider);
    return this;
  }

  /**
   * Register a Redis-backed stream provider (durable, cluster-shared). The
   * client connects when the silo starts and disconnects when it stops; the
   * provider's poll loops are stopped on shutdown. `keyPrefix` namespaces keys
   * (defaults to `"tsva"`).
   */
  addRedisStreams(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = createClient({ url: options.url });
    client.on("error", () => {});
    const provider = new RedisPullingStreamProvider(
      client,
      name,
      options.keyPrefix !== undefined ? { keyPrefix: options.keyPrefix } : {},
    );
    this.pullingStreams.push(provider);
    this.starters.push(async () => {
      await client.connect();
    });
    this.closers.push(async () => {
      provider.stop();
      await client.close();
    });
    return this.addStreamProvider(name, provider);
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

  /**
   * Register an incoming grain-call filter (Orleans `IIncomingGrainCallFilter`).
   * Filters wrap every grain-method dispatch on this silo, in registration order
   * (the first registered is the outermost); each proceeds via `context.invoke()`.
   */
  addIncomingCallFilter(filter: IncomingGrainCallFilter): this {
    this.incomingCallFilters.push(filter);
    return this;
  }

  /**
   * Register an outgoing grain-call filter (Orleans `IOutgoingGrainCallFilter`).
   * Filters wrap every outbound call made from this silo's grain references, in
   * registration order (first registered is outermost); each proceeds via
   * `context.invoke()`.
   */
  addOutgoingCallFilter(filter: OutgoingGrainCallFilter): this {
    this.outgoingCallFilters.push(filter);
    return this;
  }

  /**
   * Enable OpenTelemetry tracing: register the tracing call filters (CLIENT span
   * outgoing, SERVER span incoming) and set W3C trace-context propagation. Spans
   * are emitted through the global OpenTelemetry tracer, so register an OTel SDK
   * to export them; without one this is a no-op.
   */
  useTracing(): this {
    setupTracePropagation();
    const { incoming, outgoing } = tracingFilters();
    this.incomingCallFilters.push(incoming);
    this.outgoingCallFilters.push(outgoing);
    return this;
  }

  /** Register a transactional-storage provider for `@transactionalState` facets. */
  addTransactionalStorage(name: string, provider: TransactionalStateStorage): this {
    (this.transactionalStorage ??= new TransactionalStorageRegistry()).add(name, provider);
    return this;
  }

  /**
   * Convenience: register an in-memory "default" transactional-storage provider.
   * Sharing one instance across silo restarts stands in for a durable backend.
   */
  useMemoryTransactionalStorage(
    provider: TransactionalStateStorage = new MemoryTransactionalStorage(),
  ): this {
    return this.addTransactionalStorage("default", provider);
  }

  /**
   * Register a Redis-backed storage provider. The client connects when the silo
   * starts and disconnects when it stops; `keyPrefix` namespaces keys in a Redis
   * shared by several clusters (defaults to `"tsva"`).
   */
  addRedisStorage(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = createClient({ url: options.url });
    client.on("error", () => {}); // surfaced by connect()/commands; don't crash the process
    this.starters.push(async () => {
      await client.connect();
    });
    this.closers.push(async () => {
      await client.close();
    });
    return this.addStorage(
      name,
      new RedisGrainStorage(
        client,
        options.keyPrefix !== undefined ? { keyPrefix: options.keyPrefix } : {},
      ),
    );
  }

  /**
   * Register a Redis-backed transactional-storage provider for
   * `@transactionalState` facets. Connects on start, disconnects on stop;
   * `keyPrefix` namespaces keys (defaults to `"tsva"`).
   */
  addRedisTransactionalStorage(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = createClient({ url: options.url });
    client.on("error", () => {});
    this.starters.push(async () => {
      await client.connect();
    });
    this.closers.push(async () => {
      await client.close();
    });
    return this.addTransactionalStorage(
      name,
      new RedisTransactionalStorage(
        client,
        options.keyPrefix !== undefined ? { keyPrefix: options.keyPrefix } : {},
      ),
    );
  }

  useStaticMembership(silos: readonly SiloAddress[]): this {
    this.membership = new StaticMembershipService(this.config.local, silos);
    return this;
  }

  /**
   * Inject a membership service directly. Use this to share one view across
   * several in-process silos (so a view change reaches all of them) or to supply
   * a custom provider; `useStaticMembership` / `useKubernetesMembership` are the
   * common cases.
   */
  useMembership(service: MembershipService): this {
    this.membership = service;
    return this;
  }

  /**
   * Drive membership from Kubernetes EndpointSlices. Pass a `KubernetesEndpointWatch`
   * (built from `createKubernetesClientSource`); its `start`/`stop` lifecycle is run
   * with the silo so the watch is established before the node joins and torn down on
   * drain.
   */
  useKubernetesMembership(watch: EndpointWatch, options?: KubernetesMembershipOptions): this {
    this.membership = new KubernetesMembership(this.config.local, watch, options);
    const lifecycle = watch as Partial<{ start(): Promise<void>; stop(): void }>;
    if (typeof lifecycle.start === "function") {
      this.starters.push(() => lifecycle.start!());
    }
    if (typeof lifecycle.stop === "function") {
      this.closers.push(async () => lifecycle.stop!());
    }
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
    // Transactional facets always have a provider; default to a per-silo
    // in-memory one when none is configured (non-durable across restarts).
    const transactionalStorage =
      this.transactionalStorage ??
      new TransactionalStorageRegistry().add("default", new MemoryTransactionalStorage());
    const time = this.config.time;
    let reminderService: LocalReminderService | undefined;

    const node = new ClusterNode({
      local: this.config.local,
      clusterId: this.config.clusterId,
      membership: this.membership,
      transport: this.transport,
      ...(time !== undefined ? { time } : {}),
      ...(this.config.collectionAgeSeconds !== undefined
        ? { defaultCollectionAgeSeconds: this.config.collectionAgeSeconds }
        : {}),
      ...(this.config.collectionIntervalSeconds !== undefined
        ? { collectionIntervalSeconds: this.config.collectionIntervalSeconds }
        : {}),
      ...(this.config.random !== undefined ? { random: this.config.random } : {}),
      ...(this.incomingCallFilters.length > 0
        ? { incomingCallFilters: this.incomingCallFilters }
        : {}),
      ...(this.outgoingCallFilters.length > 0
        ? { outgoingCallFilters: this.outgoingCallFilters }
        : {}),
      // Transactional facets need no storage provider in this slice, so the
      // binder always runs; persistent/reducer facets bind only when storage is
      // configured.
      stateBinder: async (instance, grainId) => {
        if (storage !== undefined) {
          await bindPersistentStates(instance, grainId, storage);
          await bindReducerStates(instance, grainId, storage);
        }
        await bindTransactionalStates(instance, grainId, transactionalStorage, (manager, txId) =>
          node.resolveTransactionStatus(manager, txId),
        );
      },
      ...(this.reminderTable !== undefined ? { reminderRegistry: () => reminderService } : {}),
      ...(this.streamProviders.size > 0
        ? {
            streamProvider: (name?: string) =>
              this.streamProviders.get(name ?? "default") ?? this.streamProviders.get("default"),
          }
        : {}),
    });
    for (const r of this.registrations) node.registerGrain(r.ctor, { interfaces: r.interfaces });

    if (this.reminderTable !== undefined) {
      reminderService = new LocalReminderService(
        this.reminderTable,
        time ?? systemTimeProvider,
        (grainId, name, status) => node.deliverReminder(grainId, name, status),
        node.ownedHashRanges(),
        (this.config.reminderRefreshSeconds ?? 60) * 1000,
      );
    }

    // Pulling-agent stream providers deliver through the dispatcher to subscriber
    // activations, so they need the started node; the host runs the ownership hook
    // on start and on every membership change, so each silo runs agents only for
    // the queues the ring assigns it and takes queues over on rebalance.
    const onOwnershipChange: Array<
      (ranges: ReadonlyArray<readonly [number, number]>) => Promise<void>
    > = [];
    for (const provider of this.pullingStreams) {
      provider.setDeliver((grainId, streamKey, event, token) =>
        node.deliverStreamEvent(grainId, streamKey, event, token),
      );
      onOwnershipChange.push(async (ranges) => provider.refreshOwnership(ranges));
    }

    return buildSiloHost({
      node,
      health,
      healthServer: this.healthPort !== undefined ? new HealthServer(health) : undefined,
      healthPort: this.healthPort,
      membership: this.membership,
      reminderService,
      onStart: this.starters,
      onOwnershipChange,
      onStop: this.closers,
    });
  }
}

export function createSilo(config: SiloConfig): SiloBuilder {
  return new SiloBuilder(config);
}
