import { durationToMs, type Duration } from "@tsva/core/duration";
import type { Grain } from "@tsva/core/grain";
import type {
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { CompatibilityKind } from "@tsva/core/version-compatibility";
import type { VersionSelectorKind } from "@tsva/core/version-selector";
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
import { Pool } from "pg";
import { MemoryGrainStorage } from "@tsva/persistence/memory-grain-storage";
import { PostgresGrainStorage } from "@tsva/persistence/postgres-grain-storage";
import { RedisGrainStorage } from "@tsva/persistence/redis-grain-storage";
import { bindPersistentStates } from "@tsva/persistence/state-activator";
import { bindReducerStates } from "@tsva/persistence/reducer-state-activator";
import { StorageRegistry } from "@tsva/persistence/storage-registry";
import { bindGrainFacets } from "@tsva/persistence/grain-facet-activator";
import { GrainFacetRegistry, type GrainFacetFactory } from "@tsva/persistence/grain-facet-registry";
import { PlacementFilterRegistry } from "@tsva/runtime/placement/placement-filter-registry";
import type { PlacementFilter } from "@tsva/runtime/placement/placement-filter";
import { bindTransactionalStates } from "@tsva/transactions/transactional-state-activator";
import type { Logger } from "@tsva/core/logger";
import { setupTracePropagation, tracingFilters } from "@tsva/observability/tracing";
import { metricsFilters } from "@tsva/observability/metrics";
import { registerRuntimeMetrics } from "@tsva/observability/runtime-metrics";
import { loggingFilter } from "@tsva/observability/logging";
import { MemoryTransactionalStorage } from "@tsva/transactions/memory-transactional-storage";
import { RedisTransactionalStorage } from "@tsva/transactions/redis-transactional-storage";
import { TransactionalStorageRegistry } from "@tsva/transactions/transactional-storage-registry";
import type { TransactionalStateStorage } from "@tsva/core/transactional-storage";
import type { JournalStorage } from "@tsva/core/journal-storage";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { RedisJournalStorage } from "@tsva/journaling/redis-journal-storage";
import { JournalStorageRegistry } from "@tsva/journaling/journal-storage-registry";
import { bindDurableStates } from "@tsva/journaling/durable-state-activator";
import { bindJournaledGrain } from "@tsva/journaling/journaled-grain-binder";
import type { BroadcastChannelOptions } from "@tsva/core/broadcast-channel";
import type { StreamFilter, StreamProvider } from "@tsva/core/stream";
import type { DurableJobsOptions } from "@tsva/core/durable-job";
import { activeSilos } from "@tsva/core/membership";
import {
  LocalDurableJobManager,
  resolveOptions as resolveDurableJobOptions,
} from "@tsva/durable-jobs/local-durable-job-manager";
import type { JobShardStore } from "@tsva/durable-jobs/job-shard-store";
import { MemoryJobShardStore } from "@tsva/durable-jobs/memory-job-shard-store";
import { RedisJobShardStore } from "@tsva/durable-jobs/redis-job-shard-store";
import { LocalReminderService } from "@tsva/reminders/local-reminder-service";
import { MemoryReminderTable } from "@tsva/reminders/memory-reminder-table";
import { PostgresReminderTable } from "@tsva/reminders/postgres-reminder-table";
import { RedisReminderTable } from "@tsva/reminders/redis-reminder-table";
import { MemoryStreamProvider } from "@tsva/streams/memory-stream-provider";
import { RedisPullingStreamProvider } from "@tsva/streams/redis-pulling-stream-provider";
import {
  GeneratorPullingStreamProvider,
  type GeneratorPullingStreamProviderOptions,
} from "@tsva/streams/generator-pulling-stream-provider";
import type { StreamGeneratorConfig } from "@tsva/streams/generator-stream-queue";
import type { PullingStreamProviderHost } from "@tsva/streams/queue-pulling-agent";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import type { GrainActivator } from "@tsva/runtime/catalog";
import type { LoadSheddingOptions } from "@tsva/runtime/load-shedding";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { ActivationRebalancerWorker } from "@tsva/runtime/placement/rebalancing/rebalancer-worker";
import type { ImbalanceToleranceRule } from "@tsva/runtime/placement/repartitioning/activation-repartitioner";
import {
  defaultActivationRepartitionerOptions,
  validateActivationRepartitionerOptions,
  type ActivationRepartitionerOptions,
} from "@tsva/runtime/placement/repartitioning/activation-repartitioner-options";
import {
  DEFAULT_REBALANCER_OPTIONS,
  type RebalancerOptions,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import { HealthCheck } from "@tsva/hosting/health-check";
import { HealthServer } from "@tsva/hosting/health-server";
import { buildSiloHost, type SiloHost } from "@tsva/hosting/silo-host";

export interface SiloConfig {
  clusterId: string;
  /**
   * Stable logical service identity (Orleans `ServiceId`), distinct from
   * `clusterId` (a specific deployment): configured once and expected to
   * persist across silo restarts/redeployments. Defaults to `clusterId` when
   * omitted, mirroring Orleans' fallback.
   */
  serviceId?: string;
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
  /** Static metadata this silo advertises (e.g. `{ role: "worker" }`), for metadata-aware placement. */
  metadata?: Readonly<Record<string, string>>;
  /** Durable-journal snapshot threshold: snapshot + truncate past this many log entries (default 100). */
  snapshotThreshold?: number;
  /**
   * Silo-wide default per-method response timeout (Orleans `[ResponseTimeout]`
   * default), applied to any grain call whose interface doesn't set its own
   * `responseTimeout`. Off by default: no call races a deadline unless one is
   * configured here or on the method itself.
   */
  defaultResponseTimeout?: Duration;
  /**
   * Load-shedding config (Orleans `Configure<LoadSheddingOptions>`). Defaults
   * to shedding disabled; see `ClusterNodeOptions.loadShedding`.
   */
  loadShedding?: Partial<LoadSheddingOptions>;
  /**
   * Dev-mode `@readOnly` mutation guard (`GAP-READONLY-ENFORCEMENT`): when
   * `true`, a grain's `@persistentState` fields are proxy-guarded for the
   * duration of any `readOnly` call, and a mutation attempt — replacing
   * `value`, mutating a property reached through it, or calling
   * `write()`/`clear()` — throws `ReadOnlyStateViolationError` instead of
   * silently succeeding. Opt-in and `false` by default (leave it off in
   * production: the deep proxy costs a wrap per `readOnly` turn), for
   * catching a mistaken write in dev/test where `@readOnly` is otherwise only
   * advisory.
   */
  readOnlyStateGuard?: boolean;
}

interface Registration {
  ctor: new () => Grain;
  interfaces: GrainInterface<unknown>[];
}

/** Grain-factory access handed to startup tasks (Orleans `IGrainFactory`, trimmed to `getGrain`). */
export interface GrainFactoryAccess {
  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T;
}

/**
 * Build a single-key config object from an optional value, omitting the key
 * when the value is `undefined`. A defined empty string is preserved (passed
 * through as `{ [key]: "" }`), matching `!== undefined` semantics under
 * `exactOptionalPropertyTypes`.
 */
function toConfigOption<K extends string, V>(
  key: K,
  value: V | undefined,
): Record<K, V> | Record<string, never> {
  return value !== undefined ? ({ [key]: value } as Record<K, V>) : {};
}

/** Fluent builder for a silo, mirroring the hosting surface in docs/11. */
export class SiloBuilder {
  private membership: MembershipService | undefined;
  private transport: Transport | undefined;
  private healthPort: number | undefined;
  private storage: StorageRegistry | undefined;
  private transactionalStorage: TransactionalStorageRegistry | undefined;
  private journalStorage: JournalStorageRegistry | undefined;
  private grainFacets: GrainFacetRegistry | undefined;
  private placementFilters: PlacementFilterRegistry | undefined;
  private reminderTable: ReminderTable | undefined;
  private jobShardStore: JobShardStore | undefined;
  private durableJobsOptions: DurableJobsOptions = {};
  private rebalancing: { options: RebalancerOptions; sessionCyclePeriodMs: number } | undefined;
  private repartitioning:
    | { options: ActivationRepartitionerOptions; toleranceRule?: ImbalanceToleranceRule }
    | undefined;
  private readonly streamProviders = new Map<string, StreamProvider>();
  private readonly incomingCallFilters: IncomingGrainCallFilter[] = [];
  private readonly outgoingCallFilters: OutgoingGrainCallFilter[] = [];
  private grainActivator: GrainActivator | undefined;
  private metricsEnabled = false;
  private readonly registrations: Registration[] = [];
  private readonly grainExtensions = new Map<number, () => object>();
  private versioning:
    | { compatibility?: CompatibilityKind; selector?: VersionSelectorKind }
    | undefined;
  private readonly starters: Array<() => Promise<void>> = [];
  private readonly closers: Array<() => Promise<void>> = [];
  private readonly startupTasks: Array<(grains: GrainFactoryAccess) => Promise<void>> = [];
  private readonly pullingStreams: PullingStreamProviderHost[] = [];
  private readonly memoryStreams: Array<{
    provider: MemoryStreamProvider;
    options: { streamInactivityPeriodMs?: number } | undefined;
  }> = [];
  private readonly streamFilters: Array<{ providerName: string; filter: StreamFilter }> = [];
  private readonly broadcastProviders = new Map<string, BroadcastChannelOptions>();
  private transactionsDisabled = false;

  constructor(private readonly config: SiloConfig) {}

  /**
   * Create a Redis client, attach a no-op error handler (errors surface through
   * `connect()`/commands; we don't crash the process), and register its
   * connect/close on the silo's start/stop lifecycle hooks.
   */
  private createManagedRedisClient(options: { url: string }): ReturnType<typeof createClient> {
    const client = createClient({ url: options.url });
    client.on("error", () => {});
    this.starters.push(async () => {
      await client.connect();
    });
    this.closers.push(async () => {
      await client.close();
    });
    return client;
  }

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
    const client = this.createManagedRedisClient(options);
    this.reminderTable = new RedisReminderTable(
      client,
      toConfigOption("keyPrefix", options.keyPrefix),
    );
    return this;
  }

  /**
   * Enable durable reminders backed by Postgres. The connection pool is created
   * here, the reminder table is created on silo start (`start()`), and the pool
   * is closed on stop; `tableName` namespaces rows (defaults to `"tsva_reminders"`).
   */
  usePostgresReminders(options: { connectionString: string; tableName?: string }): this {
    const pool = new Pool({ connectionString: options.connectionString });
    pool.on("error", () => {});
    const table = new PostgresReminderTable(pool, toConfigOption("tableName", options.tableName));
    this.starters.push(async () => {
      await table.start();
    });
    this.closers.push(async () => {
      await pool.end();
    });
    this.reminderTable = table;
    return this;
  }

  /**
   * Enable durable jobs backed by an in-memory shard store (dev/test).
   * A single store instance shared across silo restarts stands in for a durable
   * backend. `options` mirrors Orleans `DurableJobsOptions`.
   */
  useMemoryDurableJobs(
    store: JobShardStore = new MemoryJobShardStore(),
    options: DurableJobsOptions = {},
  ): this {
    this.jobShardStore = store;
    this.durableJobsOptions = options;
    return this;
  }

  /**
   * Enable durable jobs backed by Redis (the default store). The client
   * connects when the silo starts and disconnects when it stops; `keyPrefix`
   * namespaces keys (defaults to `"tsva"`). `options` mirrors Orleans
   * `DurableJobsOptions`.
   */
  useRedisDurableJobs(options: {
    url: string;
    keyPrefix?: string;
    jobs?: DurableJobsOptions;
  }): this {
    const client = this.createManagedRedisClient(options);
    this.jobShardStore = new RedisJobShardStore(
      client,
      toConfigOption("keyPrefix", options.keyPrefix),
    );
    this.durableJobsOptions = options.jobs ?? {};
    return this;
  }

  /**
   * Enable the adaptive activation rebalancer. A single, deterministically
   * elected silo runs entropy-minimizing rebalancing cycles on a timer, migrating live
   * activations from busier to quieter silos so the cluster self-levels. `options`
   * overrides the entropy-model tunables (defaults from `DEFAULT_REBALANCER_OPTIONS`);
   * `sessionCyclePeriodSeconds` sets the cycle cadence (defaults to 60s).
   */
  useActivationRebalancing(
    options?: Partial<RebalancerOptions> & { sessionCyclePeriodSeconds?: number },
  ): this {
    const { sessionCyclePeriodSeconds, ...modelOptions } = options ?? {};
    this.rebalancing = {
      options: { ...DEFAULT_REBALANCER_OPTIONS, ...modelOptions },
      sessionCyclePeriodMs: (sessionCyclePeriodSeconds ?? 60) * 1000,
    };
    return this;
  }

  /**
   * Enable the activation repartitioner (Orleans `AddActivationRepartitioner`):
   * a communication-graph-driven protocol, distinct from the rebalancer, that
   * moves grain activations near their most frequent communication partners.
   * `toleranceRule` defaults to the cluster-size-aware
   * `rebalancerCompatibleTolerance`; pass a custom one (Orleans
   * `AddActivationRepartitioner<TRule>`) to control the tolerated imbalance
   * between an exchanging silo pair directly.
   */
  useActivationRepartitioning(
    options?: Partial<ActivationRepartitionerOptions>,
    toleranceRule?: ImbalanceToleranceRule,
  ): this {
    const resolved = { ...defaultActivationRepartitionerOptions(), ...options };
    validateActivationRepartitionerOptions(resolved);
    this.repartitioning = {
      options: resolved,
      ...(toleranceRule !== undefined ? { toleranceRule } : {}),
    };
    return this;
  }

  /**
   * Register a stream provider (in-memory by default), named "default" unless
   * given. `options.streamInactivityPeriodMs` mirrors Orleans'
   * `ConfigurePullingAgent(... StreamInactivityPeriod ...)`: how long a stream
   * may go without a publish before it is reported inactive (see
   * `MemoryStreamProvider.setStreamInactivityPeriod`). Orleans' cache-eviction
   * options (`DataMaxAgeInCache`, `DataMinTimeInCache`, `MetadataMinTimeInCache`,
   * configured via `ConfigureCacheEviction`) are intentionally NOT exposed
   * here: this provider delivers by direct push, in-process, with no bounded
   * cache to evict from — see `MemoryStreamProvider`'s class doc.
   */
  useMemoryStreams(name = "default", options?: { streamInactivityPeriodMs?: number }): this {
    const provider = new MemoryStreamProvider(name);
    this.memoryStreams.push({ provider, options });
    this.streamProviders.set(name, provider);
    return this;
  }

  /**
   * Register a server-side stream filter (Orleans `ISiloBuilder.AddStreamFilter`)
   * on the named memory stream provider: suppresses delivery of an item to
   * every implicit subscriber before it reaches one, based on the item's
   * content (see `StreamFilter`). Applies only to a provider registered via
   * `useMemoryStreams` with a matching name.
   */
  addStreamFilter(providerName: string, filter: StreamFilter): this {
    this.streamFilters.push({ providerName, filter });
    return this;
  }

  addStreamProvider(name: string, provider: StreamProvider): this {
    this.streamProviders.set(name, provider);
    return this;
  }

  /**
   * Register a broadcast-channel provider, named "default" unless given.
   * Broadcast channels are direct in-cluster pub/sub with no backing store —
   * a publish fans the item out to the channel's implicit subscribers — so
   * there is nothing to connect or tear down; only the name (and its
   * `fireAndForgetDelivery` setting, Orleans `BroadcastChannelOptions`,
   * default `false` here — see `ClusterNode`'s constructor) is registered.
   */
  useBroadcastChannels(name = "default", options: BroadcastChannelOptions = {}): this {
    this.broadcastProviders.set(name, options);
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
      toConfigOption("keyPrefix", options.keyPrefix),
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

  /**
   * Register a test-only generator stream provider (Orleans
   * `GeneratorAdapterFactory`): each of `options.queueCount` physical queues
   * synthesizes one stream of `config.eventsInStream` events under
   * `config.streamNamespace`, delivered to implicit subscribers through the
   * same pulling-agent/queue-ownership path as `addRedisStreams` — no real
   * backing store, so there is nothing to connect; only agents to stop on
   * shutdown.
   */
  addGeneratorStreams(
    name: string,
    config: StreamGeneratorConfig,
    options: GeneratorPullingStreamProviderOptions = {},
  ): this {
    const provider = new GeneratorPullingStreamProvider(name, config, options);
    this.pullingStreams.push(provider);
    this.closers.push(async () => {
      provider.stop();
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
   * Register a named factory for a third-party grain facet family (Orleans'
   * `IAttributeToFactoryMapper` extensibility point — e.g. an `ExampleStorage`
   * plugin's `UseBlobExampleStorage`/`UseTableExampleStorage`). `kind` names
   * the facet family; a grain's `@grainFacet(kind, name)` field (or a
   * plugin-specific decorator built on it) resolves to the factory registered
   * here under that `name`. Register a factory under `"default"` to make it
   * the fallback for a `@grainFacet` field that names no provider (Orleans'
   * `UseAsDefaultExampleStorage`).
   */
  addGrainFacetFactory(kind: string, name: string, factory: GrainFacetFactory): this {
    (this.grainFacets ??= new GrainFacetRegistry()).add(kind, name, factory);
    return this;
  }

  /**
   * Register a named custom placement-filter director (Orleans
   * `IServiceCollection.AddPlacementFilter<TStrategy, TDirector>`). A grain
   * opts in via a `{ kind: "custom", name, order }` entry in its
   * `placementFilters`, resolved here by `name` at placement time.
   */
  addPlacementFilter(name: string, director: PlacementFilter): this {
    (this.placementFilters ??= new PlacementFilterRegistry()).add(name, director);
    return this;
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
   * Install a custom grain activator (Orleans `IGrainActivator`): a hook to
   * construct/dispose grain instances instead of `new ctor()`, e.g. object
   * pooling or non-DI construction.
   */
  useGrainActivator(activator: GrainActivator): this {
    this.grainActivator = activator;
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

  /**
   * Enable OpenTelemetry metrics: register the metrics call filter (a grain-call
   * counter and call-duration histogram). Emitted through the global OpenTelemetry
   * meter — register an OTel SDK to export them; without one this is a no-op.
   */
  useMetrics(): this {
    this.incomingCallFilters.push(metricsFilters().incoming);
    this.metricsEnabled = true;
    return this;
  }

  /**
   * Log each incoming grain call (and its failures) with structured fields via
   * the given {@link Logger}, on the call-filter seam.
   */
  useLogging(logger: Logger): this {
    this.incomingCallFilters.push(loggingFilter(logger));
    return this;
  }

  /**
   * Enable grain-interface versioning: version-aware placement steers
   * a new activation onto a silo whose implemented interface version is
   * compatible with the caller's. `compatibility` defaults to `"backwardCompatible"`
   * (a newer silo can serve an older caller); `selector` defaults to `"latest"`.
   * When no silo is compatible, placement falls back to the full candidate set.
   */
  useVersioning(
    options: { compatibility?: CompatibilityKind; selector?: VersionSelectorKind } = {},
  ): this {
    this.versioning = options;
    return this;
  }

  /** Register a transactional-storage provider for `@transactionalState` facets. */
  addTransactionalStorage(name: string, provider: TransactionalStateStorage): this {
    (this.transactionalStorage ??= new TransactionalStorageRegistry()).add(name, provider);
    return this;
  }

  /**
   * Build this silo with transactions disabled (Orleans: no `.UseTransactions()`
   * call). No transactional-storage provider is wired — not even the default
   * in-memory one — so any `[Transaction]`-style grain call throws
   * `TransactionsDisabledError`. Mutually exclusive with configuring
   * transactional storage; call before any `addTransactionalStorage` /
   * `useMemoryTransactionalStorage` / `addRedisTransactionalStorage` call, or
   * not at all.
   */
  disableTransactions(): this {
    this.transactionsDisabled = true;
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
    const client = this.createManagedRedisClient(options);
    return this.addStorage(
      name,
      new RedisGrainStorage(client, toConfigOption("keyPrefix", options.keyPrefix)),
    );
  }

  /**
   * Register a Postgres-backed storage provider. The connection pool is created
   * here, the backing table is created on silo start (`start()`), and the pool is
   * closed on stop; `tableName` namespaces rows in a Postgres shared by several
   * clusters (defaults to `"tsva_grain_state"`). Good when state must also be
   * queried outside the actor model.
   */
  addPostgresStorage(
    name: string,
    options: { connectionString: string; tableName?: string },
  ): this {
    const pool = new Pool({ connectionString: options.connectionString });
    pool.on("error", () => {}); // surfaced by start()/queries; don't crash the process
    const provider = new PostgresGrainStorage(pool, toConfigOption("tableName", options.tableName));
    this.starters.push(async () => {
      await provider.start();
    });
    this.closers.push(async () => {
      await pool.end();
    });
    return this.addStorage(name, provider);
  }

  /**
   * Register a Redis-backed transactional-storage provider for
   * `@transactionalState` facets. Connects on start, disconnects on stop;
   * `keyPrefix` namespaces keys (defaults to `"tsva"`).
   */
  addRedisTransactionalStorage(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = this.createManagedRedisClient(options);
    return this.addTransactionalStorage(
      name,
      new RedisTransactionalStorage(client, toConfigOption("keyPrefix", options.keyPrefix)),
    );
  }

  /** Register a named journal-storage provider for durable-journalling facets. */
  addJournaling(name: string, provider: JournalStorage): this {
    (this.journalStorage ??= new JournalStorageRegistry()).add(name, provider);
    return this;
  }

  /**
   * Convenience: register an in-memory "default" journal provider (dev/tests).
   * Sharing one instance across silo restarts stands in for a durable backend.
   */
  useMemoryJournaling(provider: JournalStorage = new MemoryJournalStorage()): this {
    return this.addJournaling("default", provider);
  }

  /**
   * Register a Redis-backed journal provider for durable-journalling facets. The
   * client connects when the silo starts and disconnects when it stops;
   * `keyPrefix` namespaces keys (defaults to `"tsva"`).
   */
  addRedisJournaling(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = this.createManagedRedisClient(options);
    return this.addJournaling(
      name,
      new RedisJournalStorage(client, toConfigOption("keyPrefix", options.keyPrefix)),
    );
  }

  useStaticMembership(
    silos: readonly SiloAddress[],
    metadata?: (silo: SiloAddress) => Readonly<Record<string, string>> | undefined,
  ): this {
    this.membership = new StaticMembershipService(this.config.local, silos, metadata);
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
    // TODO: populate SiloMember.metadata from pod labels so role/metadata-aware
    // placement works under Kubernetes membership (deferred; static membership
    // carries metadata today).
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

  /**
   * Register an auto-install factory for a `GrainExtension` interface (Orleans
   * `ISiloBuilder.AddGrainExtension<TExtensionInterface, TExtension>`): any
   * activation on this silo that receives a call for `iface` and has not
   * already bound one itself (via `GrainRuntime.getOrSetExtension`) gets one
   * built by `factory` on first use, instead of throwing
   * `GrainExtensionNotInstalledException`.
   */
  addGrainExtension<T>(iface: GrainInterface<T>, factory: () => object): this {
    this.grainExtensions.set(iface.id, factory);
    return this;
  }

  /**
   * Register a startup task (Orleans `ISiloBuilder.AddStartupTask` / `IStartupTask`):
   * runs after the silo's node has started — so it can call grains — but before
   * the silo is marked ready. Tasks run in registration order.
   */
  addStartupTask(fn: (grains: GrainFactoryAccess) => Promise<void>): this {
    this.startupTasks.push(fn);
    return this;
  }

  build(): SiloHost {
    if (this.membership === undefined) throw new Error("silo: no membership configured");
    if (this.transport === undefined) throw new Error("silo: no transport configured");
    const health = new HealthCheck();
    const storage = this.storage;
    // Transactional facets default to a per-silo in-memory provider
    // (non-durable across restarts) unless transactions were explicitly
    // disabled via `disableTransactions()`, in which case no provider is
    // wired at all and `[Transaction]`-style calls fail (Orleans parity:
    // transactions are opt-in).
    const transactionalStorage = this.transactionsDisabled
      ? undefined
      : (this.transactionalStorage ??
        new TransactionalStorageRegistry().add("default", new MemoryTransactionalStorage()));
    const journalStorage = this.journalStorage;
    const grainFacets = this.grainFacets;
    const snapshotThreshold = this.config.snapshotThreshold;
    const time = this.config.time;
    let reminderService: LocalReminderService | undefined;
    let jobManager: LocalDurableJobManager | undefined;
    const membership = this.membership;

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
      ...(this.config.defaultResponseTimeout !== undefined
        ? { defaultResponseTimeoutMs: durationToMs(this.config.defaultResponseTimeout) }
        : {}),
      ...(this.config.random !== undefined ? { random: this.config.random } : {}),
      // Calling useVersioning() enables versioning with resolved defaults, so the
      // node activates version-aware placement even when no policy is overridden.
      ...(this.versioning !== undefined
        ? {
            versionCompatibility: this.versioning.compatibility ?? "backwardCompatible",
            versionSelector: this.versioning.selector ?? "latest",
          }
        : {}),
      ...(this.config.metadata !== undefined ? { metadata: this.config.metadata } : {}),
      ...(this.config.loadShedding !== undefined ? { loadShedding: this.config.loadShedding } : {}),
      ...(this.config.readOnlyStateGuard !== undefined
        ? { readOnlyStateGuard: this.config.readOnlyStateGuard }
        : {}),
      ...(this.repartitioning !== undefined ? { repartitioning: this.repartitioning } : {}),
      transactionsEnabled: transactionalStorage !== undefined,
      ...(this.broadcastProviders.size > 0
        ? {
            broadcastProviders: [...this.broadcastProviders].map(([name, options]) => ({
              name,
              ...options,
            })),
          }
        : {}),
      ...(this.incomingCallFilters.length > 0
        ? { incomingCallFilters: this.incomingCallFilters }
        : {}),
      ...(this.outgoingCallFilters.length > 0
        ? { outgoingCallFilters: this.outgoingCallFilters }
        : {}),
      ...(this.grainActivator !== undefined ? { grainActivator: this.grainActivator } : {}),
      ...(this.grainExtensions.size > 0 ? { grainExtensionFactories: this.grainExtensions } : {}),
      ...(this.placementFilters !== undefined
        ? { placementFilterRegistry: this.placementFilters }
        : {}),
      // Transactional facets need no storage provider in this slice, so the
      // binder always runs; persistent/reducer facets bind only when storage is
      // configured.
      stateBinder: async (instance, grainId, mode) => {
        if (storage !== undefined) {
          // On rehydration the migrated value is restored from the bag, so bind the
          // persistent facet without reading storage (which would clobber it).
          await bindPersistentStates(instance, grainId, storage, { read: mode !== "rehydrate" });
          await bindReducerStates(instance, grainId, storage);
        }
        if (journalStorage !== undefined) {
          // One manager per grain owns the log; replay rebuilds all durable
          // structures. On rehydration skip replay (parity with persistent state).
          await bindDurableStates(instance, grainId, journalStorage, {
            replay: mode !== "rehydrate",
            ...(snapshotThreshold !== undefined ? { snapshotThreshold } : {}),
          });
          // A `JournaledGrain` owns its own single-machine log (the confirmed
          // event sequence); install its adaptor and replay it the same way.
          await bindJournaledGrain(instance, grainId, journalStorage, {
            replay: mode !== "rehydrate",
            ...(snapshotThreshold !== undefined ? { snapshotThreshold } : {}),
          });
        }
        if (transactionalStorage !== undefined) {
          await bindTransactionalStates(instance, grainId, transactionalStorage, (manager, txId) =>
            node.resolveTransactionStatus(manager, txId),
          );
        }
        if (grainFacets !== undefined) {
          await bindGrainFacets(instance, grainId, grainFacets);
        }
      },
      ...(this.reminderTable !== undefined ? { reminderRegistry: () => reminderService } : {}),
      ...(this.jobShardStore !== undefined ? { durableJobScheduler: () => jobManager } : {}),
      ...(this.streamProviders.size > 0
        ? {
            streamProvider: (name?: string) =>
              this.streamProviders.get(name ?? "default") ?? this.streamProviders.get("default"),
          }
        : {}),
    });
    for (const r of this.registrations) node.registerGrain(r.ctor, { interfaces: r.interfaces });

    if (this.metricsEnabled) {
      const unregister = registerRuntimeMetrics({
        activationCount: () => node.activationCount(),
        directoryCache: () => node.directoryCacheStats(),
      });
      this.closers.push(async () => unregister());
    }

    if (this.reminderTable !== undefined) {
      reminderService = new LocalReminderService(
        this.reminderTable,
        time ?? systemTimeProvider,
        (grainId, name, status) => node.deliverReminder(grainId, name, status),
        node.ownedHashRanges(),
        (this.config.reminderRefreshSeconds ?? 60) * 1000,
      );
    }

    // The rebalancer worker elects a single leader from membership and
    // drives the node's `runRebalanceCycle` on a timer; the host starts/stops it.
    let rebalancerWorker: ActivationRebalancerWorker | undefined;
    if (this.rebalancing !== undefined) {
      const rebalancing = this.rebalancing;
      rebalancerWorker = new ActivationRebalancerWorker({
        local: this.config.local,
        membership: this.membership,
        time: time ?? systemTimeProvider,
        runCycle: (state) => node.runRebalanceCycle(state, rebalancing.options),
        options: rebalancing.options,
        sessionCyclePeriodMs: rebalancing.sessionCyclePeriodMs,
      });
    }

    // Pulling-agent stream providers deliver through the dispatcher to subscriber
    // activations, so they need the started node; the host runs the ownership hook
    // on start and on every membership change, so each silo runs agents only for
    // the queues the ring assigns it and takes queues over on rebalance.
    const onOwnershipChange: Array<
      (ranges: ReadonlyArray<readonly [number, number]>) => Promise<void>
    > = [];

    // Durable jobs: the per-silo manager runs the executors for the
    // time-bucketed shards this silo owns, delivering each due job as a turn on
    // its target through the node. Ownership is membership-driven (not hash-range),
    // so the ownership hook recomputes the active set on start and every view
    // change and reconciles shard claims/adoptions/drops.
    if (this.jobShardStore !== undefined) {
      jobManager = new LocalDurableJobManager(
        this.jobShardStore,
        time ?? systemTimeProvider,
        (job) => node.deliverDurableJob(job),
        resolveDurableJobOptions(this.durableJobsOptions),
        {
          localRingKey: this.config.local.ringKey,
          activeRingKeys: activeSilos(membership.current()).map((s) => s.ringKey),
        },
      );
      const manager = jobManager;
      onOwnershipChange.push(async () => {
        await manager.refreshOwnership({
          localRingKey: this.config.local.ringKey,
          activeRingKeys: activeSilos(membership.current()).map((s) => s.ringKey),
        });
      });
      // Graceful drain releases this silo's shards so a successor can claim them.
      this.closers.push(async () => manager.stop());
    }
    for (const provider of this.pullingStreams) {
      provider.setDeliver((grainId, streamKey, event, token) =>
        node.deliverStreamEvent(grainId, streamKey, event, token),
      );
      provider.setImplicitSubscribers((namespace) => node.implicitGrainTypes(namespace));
      onOwnershipChange.push(async (ranges) => provider.refreshOwnership(ranges));
    }
    // The memory provider delivers synchronously in-process on `publish` (no
    // pulling agent/queue ownership), but implicit subscribers still need the
    // same dispatcher-routed delivery pulling providers use — see
    // `MemoryStreamProvider`'s class doc.
    for (const { provider, options } of this.memoryStreams) {
      provider.setDeliver((grainId, streamKey, event, token) =>
        node.deliverStreamEvent(grainId, streamKey, event, token),
      );
      provider.setImplicitSubscribers((namespace) => node.implicitGrainTypes(namespace));
      provider.setConfirmSubscription((grainId, streamKey) =>
        node.confirmStreamSubscription(grainId, streamKey),
      );
      provider.setTimeProvider(time ?? systemTimeProvider);
      if (options?.streamInactivityPeriodMs !== undefined) {
        provider.setStreamInactivityPeriod(options.streamInactivityPeriodMs);
      }
      const filter = this.streamFilters.find((f) => f.providerName === provider.name)?.filter;
      if (filter !== undefined) provider.setStreamFilter(filter);
      // Cancel every stream's inactivity timer on shutdown so none leak past
      // this silo's lifetime (a leaked `setTimeout`/fake-clock timer would
      // otherwise hang a test worker).
      this.closers.push(async () => provider.stop());
    }

    return buildSiloHost({
      node,
      serviceId: this.config.serviceId ?? this.config.clusterId,
      health,
      healthServer: this.healthPort !== undefined ? new HealthServer(health) : undefined,
      healthPort: this.healthPort,
      membership: this.membership,
      reminderService,
      rebalancerWorker,
      onStart: this.starters,
      onOwnershipChange,
      onStop: this.closers,
      startupTasks: this.startupTasks.map((fn) => () => fn(node)),
    });
  }
}

export function createSilo(config: SiloConfig): SiloBuilder {
  return new SiloBuilder(config);
}
