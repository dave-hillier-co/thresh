import { durationToMs, type Duration } from "@thresh/core/duration";
import type { GrainClass } from "@thresh/core/grain-class";
import type { GrainRegistrationSpec } from "@thresh/core/grain-registration-spec";
import type {
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@thresh/core/grain-call-filter";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import {
  normalizeRegistration,
  type GrainRegistration,
  type Registrable,
} from "@thresh/core/grain-registration";
import type { KeyTypeOf } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import type { CompatibilityKind } from "@thresh/core/version-compatibility";
import type { VersionSelectorKind } from "@thresh/core/version-selector";
import {
  KubernetesMembership,
  type EndpointWatch,
  type KubernetesMembershipOptions,
} from "@thresh/clustering-k8s/kubernetes-membership";
import type { GrainStorage } from "@thresh/core/grain-storage";
import { InProcessTransport, type InProcessNetwork } from "@thresh/messaging/in-process-transport";
import type { Transport } from "@thresh/messaging/transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";
import { createClient as createClientNode, type ClientNode } from "@thresh/client/client-node";
import type { ReminderTable } from "@thresh/core/reminder";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import { createClient } from "redis";
import { Pool } from "pg";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { PostgresGrainStorage } from "@thresh/persistence/postgres-grain-storage";
import { RedisGrainStorage } from "@thresh/persistence/redis-grain-storage";
import { bindPersistentStates } from "@thresh/persistence/state-activator";
import { bindReducerStates } from "@thresh/persistence/reducer-state-activator";
import { StorageRegistry } from "@thresh/persistence/storage-registry";
import { bindGrainFacets } from "@thresh/persistence/grain-facet-activator";
import {
  GrainFacetRegistry,
  type GrainFacetFactory,
} from "@thresh/persistence/grain-facet-registry";
import { PlacementFilterRegistry } from "@thresh/runtime/placement/placement-filter-registry";
import { PlacementStrategyRegistry } from "@thresh/runtime/placement/placement-strategy-registry";
import type { PlacementStrategy } from "@thresh/runtime/placement/placement-strategy";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import {
  bindTransactionalStates,
  unbindTransactionalStates,
} from "@thresh/transactions/transactional-state-activator";
import type { Logger } from "@thresh/core/logger";
import { setupTracePropagation, tracingFilters } from "@thresh/observability/tracing";
import { metricsFilters } from "@thresh/observability/metrics";
import { registerRuntimeMetrics } from "@thresh/observability/runtime-metrics";
import { loggingFilter } from "@thresh/observability/logging";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { RedisTransactionalStorage } from "@thresh/transactions/redis-transactional-storage";
import { TransactionalStorageRegistry } from "@thresh/transactions/transactional-storage-registry";
import type { TransactionalStateStorage } from "@thresh/core/transactional-storage";
import type { JournalStorage } from "@thresh/core/journal-storage";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { RedisJournalStorage } from "@thresh/journaling/redis-journal-storage";
import { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { bindDurableStates } from "@thresh/journaling/durable-state-activator";
import { bindJournaledGrain } from "@thresh/journaling/journaled-grain-binder";
import type { BroadcastChannelOptions } from "@thresh/core/broadcast-channel";
import type { StreamFilter, StreamProvider } from "@thresh/core/stream";
import type { DurableJobsOptions } from "@thresh/core/durable-job";
import { activeSilos } from "@thresh/core/membership";
import {
  LocalDurableJobManager,
  resolveOptions as resolveDurableJobOptions,
} from "@thresh/durable-jobs/local-durable-job-manager";
import type { JobShardStore } from "@thresh/durable-jobs/job-shard-store";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import { RedisJobShardStore } from "@thresh/durable-jobs/redis-job-shard-store";
import {
  LocalReminderService,
  type ReminderServiceOptions,
} from "@thresh/reminders/local-reminder-service";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { PostgresReminderTable } from "@thresh/reminders/postgres-reminder-table";
import { RedisReminderTable } from "@thresh/reminders/redis-reminder-table";
import { Kafka } from "kafkajs";
import { MemoryStreamProvider } from "@thresh/streams/memory-stream-provider";
import { RedisPullingStreamProvider } from "@thresh/streams/redis-pulling-stream-provider";
import { PostgresPullingStreamProvider } from "@thresh/streams/postgres-pulling-stream-provider";
import { KafkaPullingStreamProvider } from "@thresh/streams/kafka-pulling-stream-provider";
import {
  GeneratorPullingStreamProvider,
  type GeneratorPullingStreamProviderOptions,
} from "@thresh/streams/generator-pulling-stream-provider";
import type { StreamGeneratorConfig } from "@thresh/streams/generator-stream-queue";
import type { SubscriptionRegistry } from "@thresh/streams/pulling-stream-provider-core";
import type {
  PullingStreamProviderHost,
  StreamFailureHandler,
} from "@thresh/streams/queue-pulling-agent";
import {
  DurableStreamFailureHandler,
  MemoryStreamFailureStore,
  type DurableStreamFailureStore,
} from "@thresh/streams/stream-failure-store";
import { PostgresSubscriptionRegistry } from "@thresh/streams/postgres-subscription-registry";
import { RedisSubscriptionRegistry } from "@thresh/streams/redis-subscription-registry";
import { PostgresStreamCursorStore } from "@thresh/streams/postgres-stream-cursor-store";
import { RedisStreamCursorStore } from "@thresh/streams/redis-stream-cursor-store";
import type { StreamCursorStore } from "@thresh/streams/stream-cursor-store";
import { StreamProviderConfigurationError } from "@thresh/streams/stream-provider-config-error";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import type { ActivationOptions } from "@thresh/runtime/activation";
import type { GrainActivator } from "@thresh/runtime/catalog";
import { GrainServiceRegistry, type GrainService } from "@thresh/runtime/grain-service";
import type { LoadSheddingOptions } from "@thresh/runtime/load-shedding";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { ActivationRebalancerWorker } from "@thresh/runtime/placement/rebalancing/rebalancer-worker";
import type { ImbalanceToleranceRule } from "@thresh/runtime/placement/repartitioning/activation-repartitioner";
import {
  defaultActivationRepartitionerOptions,
  validateActivationRepartitionerOptions,
  type ActivationRepartitionerOptions,
} from "@thresh/runtime/placement/repartitioning/activation-repartitioner-options";
import {
  DEFAULT_REBALANCER_OPTIONS,
  type RebalancerOptions,
} from "@thresh/runtime/placement/rebalancing/rebalancer-model";
import { HealthCheck } from "@thresh/hosting/health-check";
import { HealthServer } from "@thresh/hosting/health-server";
import { buildSiloHost, type SiloHost } from "@thresh/hosting/silo-host";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";

export interface SiloConfig {
  clusterId: string;
  /**
   * Stable logical service identity (Orleans `ServiceId`), distinct from
   * `clusterId` (a specific deployment): configured once and expected to
   * persist across silo restarts/redeployments. Defaults to `"default"`, NOT to
   * `clusterId` — Orleans' `ClusterOptions.ServiceId` has its own
   * `DefaultServiceId = "default"` and is independent of `ClusterId`, whose doc
   * says ServiceId "should survive deployment and redeployment, where as
   * ClusterId might not". Storage partitions rows by this, so defaulting it to
   * `clusterId` would silently strand every row on a redeployment that changes
   * the cluster id — which is the property ServiceId exists to provide.
   */
  serviceId?: string;
  local: SiloAddress;
  /** Shared clock (defaults to the system clock); inject a fake for tests. */
  time?: TimeProvider;
  /** Idle-deactivation threshold for grains without their own `collectionAgeSeconds`. */
  collectionAgeSeconds?: number;
  /**
   * Per-silo idle-deactivation ages keyed by GRAIN TYPE (Orleans'
   * `GrainCollectionOptions.ClassSpecificCollectionAge`). An entry overrides that
   * grain type's own `@grain({ collectionAgeSeconds })` — which remains the
   * default, and remains in sole charge for a type this map does not name — so
   * two silos in one process can hold different ages for the same grain class.
   * Key by the registered grain type (`getGrainMetadata(Ctor).grainType`), never
   * `Ctor.name`, which a bundler may rename.
   */
  classSpecificCollectionAgeSeconds?: Readonly<Record<string, number>>;
  /**
   * This silo's default placement strategy for grain types that declare no
   * `placement` in their `@grain()` metadata (Orleans' `PlacementStrategy` DI
   * singleton, registered as `RandomPlacement` by `DefaultSiloServices` and
   * replaceable per silo). An explicit per-class `placement` — `"random"`
   * included — and `stateless: true` both still win. Unset, the default stays
   * `RandomPlacement`.
   */
  defaultPlacementStrategy?: PlacementStrategy;
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
   * Per-activation turn-scheduler back-pressure/watchdog config (Orleans
   * `SchedulingOptions`). Any field left unset falls back to this builder's
   * Orleans-inspired default (see `DEFAULT_SCHEDULING_OPTIONS`) rather than
   * being disabled — unlike `ClusterNodeOptions.activationOptions`, which is
   * off entirely unless configured.
   */
  scheduling?: {
    /** Warn once an activation's turn queue reaches this length. Default 1,000. */
    maxEnqueuedRequestsSoftLimit?: number;
    /** Reject a newly scheduled turn once an activation's queue is at this length. Default 10,000. */
    maxEnqueuedRequestsHardLimit?: number;
    /** Warn once a running turn exceeds this duration. Default 30s. */
    maxRequestProcessingTime?: Duration;
  };
  /**
   * Orleans `CollectionOptions.DeactivationTimeout`: force deactivation to
   * proceed once `onDeactivate` has run this long, even if the hook itself
   * hasn't settled. Defaults to 30s; pass `false` to disable (wait
   * indefinitely, the pre-existing behaviour).
   */
  deactivationTimeout?: Duration | false;
  /**
   * Dev-mode `@readOnly` mutation guard (`GAP-READONLY-ENFORCEMENT`): when
   * `true`, a grain's persistent, reducer, durable-journaling
   * (`@durableState`/`@durableDictionary`/`@durableList`/`@durableQueue`/
   * `@durableSet`) and transactional state facets are proxy-guarded for the
   * duration of any `readOnly` call, and a mutation attempt — replacing
   * `value`, mutating a property reached through it, raising/writing a
   * reducer, mutating a durable structure, or performing a transactional
   * update — throws `ReadOnlyStateViolationError` instead of silently
   * succeeding. Opt-in and `false` by default (leave it off in production:
   * the deep proxy costs a wrap per `readOnly` turn), for catching a
   * mistaken write in dev/test where `@readOnly` is otherwise only advisory.
   */
  readOnlyStateGuard?: boolean;
}

/**
 * Orleans-inspired defaults for `SiloConfig.scheduling` /
 * `deactivationTimeout` — not verified against the exact upstream
 * `SchedulingOptions`/`CollectionOptions` constants, but in the same spirit:
 * generous enough that no well-behaved grain ever trips them, tight enough
 * that a stuck one is caught rather than growing without bound.
 */
const DEFAULT_MAX_ENQUEUED_REQUESTS_SOFT_LIMIT = 1_000;
const DEFAULT_MAX_ENQUEUED_REQUESTS_HARD_LIMIT = 10_000;
const DEFAULT_MAX_REQUEST_PROCESSING_TIME_MS = 30_000;
const DEFAULT_DEACTIVATION_TIMEOUT_MS = 30_000;

/**
 * One entry in this silo's grain-registration list. Exported (and aliased to
 * the canonical `GrainRegistrationSpec`) so a consumer can declare its own
 * shared list — the same one it hands to a cluster client — by name rather
 * than restating the shape structurally.
 */
export type Registration = GrainRegistrationSpec;

/**
 * Grain-factory access handed to startup tasks (Orleans `IGrainFactory`,
 * trimmed to `getGrain` plus the observer-hosting surface a lifecycle
 * participant needs — Orleans' `LifecycleObserverCreationTests` exercises
 * `CreateObjectReference` from exactly this hook).
 */
export interface GrainFactoryAccess {
  getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: KeyTypeOf<K>): T;
  /**
   * Host a client-style observer reachable from any grain call made during
   * this (or a later) startup task. Backed by an embedded `ClientNode` created
   * lazily on first use and gatewayed through this same silo, over whichever
   * transport the silo itself runs: in-process it takes a distinct endpoint on
   * the shared network, over WebSocket an ephemeral port on the silo's own
   * host. A silo that depends on the seam should declare
   * `SiloBuilder.requireObserverHosting()`, which moves the failure for a
   * transport that cannot back it from here to `build()`.
   */
  createObjectReference<T>(def: GrainInterface<T>, obj: object): T;
  deleteObjectReference(ref: object): void;
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
  private placementStrategies: PlacementStrategyRegistry | undefined;
  private reminderTable: ReminderTable | undefined;
  private reminderServiceOptions: ReminderServiceOptions | undefined;
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
  private readonly grainServiceFactories = new Map<string, () => GrainService>();
  private versioning:
    | { compatibility?: CompatibilityKind; selector?: VersionSelectorKind }
    | undefined;
  private readonly starters: Array<() => Promise<void>> = [];
  private readonly closers: Array<() => Promise<void>> = [];
  private readonly startupTasks: Array<(grains: GrainFactoryAccess) => Promise<void>> = [];
  private inProcessNetwork: InProcessNetwork | undefined;
  private observerHostingRequired = false;
  private readonly pullingStreams: PullingStreamProviderHost[] = [];
  private readonly memoryStreams: Array<{
    provider: MemoryStreamProvider;
    options: { streamInactivityPeriodMs?: number } | undefined;
  }> = [];
  private readonly streamFilters: Array<{ providerName: string; filter: StreamFilter }> = [];
  private streamFailureStore: DurableStreamFailureStore | undefined;
  private readonly broadcastProviders = new Map<string, BroadcastChannelOptions>();
  private transactionsDisabled = false;
  private logger: Logger | undefined;

  constructor(private readonly config: SiloConfig) {}

  /**
   * How the embedded `ClientNode` that backs `GrainFactoryAccess.createObjectReference`
   * gets onto this silo's own network, or `undefined` when the configured
   * transport cannot give it one.
   *
   * Both shapes advertise a placeholder endpoint that the client replaces with
   * its listener's BOUND address in `connect()`:
   *
   * - in-process: a distinct endpoint on the shared `InProcessNetwork`, which
   *   is just a map key, so the `#embedded-client` suffix is the whole of it;
   * - WebSocket: port `0` on the SAME host the silo itself binds, so the OS
   *   picks a free port and `WebSocketTransport.listen` reports it back. The
   *   client leg only ever has to be reachable from its gateway — this silo, in
   *   this process — because a call for a client-hosted object is routed to the
   *   client's gateway silo first (`ClusterNode.routeToClient`), never dialled
   *   directly by another silo.
   */
  private embeddedClientLeg(): { transport: Transport; address: SiloAddress } | undefined {
    const { local } = this.config;
    const address = (endpoint: string) =>
      new SiloAddress(
        `${local.podName}-embedded-client`,
        `${local.podUid}-embedded-client`,
        endpoint,
      );
    if (this.inProcessNetwork !== undefined) {
      return {
        transport: new InProcessTransport(this.inProcessNetwork, this.config.clusterId),
        address: address(`${local.endpoint}#embedded-client`),
      };
    }
    if (this.transport instanceof WebSocketTransport) {
      const host = local.endpoint.slice(0, local.endpoint.lastIndexOf(":"));
      return {
        transport: new WebSocketTransport(this.config.clusterId),
        address: address(`${host}:0`),
      };
    }
    return undefined;
  }

  /**
   * This silo's logical service identity (Orleans `ClusterOptions.ServiceId`),
   * defaulting to `DEFAULT_SERVICE_ID` exactly as Orleans does. Storage
   * providers partition their rows/keys by it, so several services can share
   * one Postgres or Redis, and `buildSiloHost` advertises it.
   *
   * The default MUST agree with what the storage providers themselves fall back
   * to, because `PostgresGrainStorage`'s migration backfills pre-service rows to
   * that same literal: a default of `clusterId` here would stamp existing rows
   * `"default"` and then read them back under the cluster id, matching nothing.
   */
  private get serviceIdentity(): string {
    return this.config.serviceId ?? DEFAULT_SERVICE_ID;
  }

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

  /**
   * Enable durable reminders backed by the given table (in-memory by default).
   * `options` mirrors Orleans `ReminderOptions` (e.g. `minimumPeriod`).
   */
  useReminders(
    table: ReminderTable = new MemoryReminderTable(),
    options?: ReminderServiceOptions,
  ): this {
    this.reminderTable = table;
    this.reminderServiceOptions = options;
    return this;
  }

  /**
   * Enable durable reminders backed by Redis. The client connects when the silo
   * starts and disconnects when it stops; `keyPrefix` namespaces keys (defaults
   * to `"thresh"`). `serviceOptions` mirrors Orleans `ReminderOptions` (e.g.
   * `minimumPeriod`).
   */
  useRedisReminders(
    options: { url: string; keyPrefix?: string },
    serviceOptions?: ReminderServiceOptions,
  ): this {
    const client = this.createManagedRedisClient(options);
    this.reminderTable = new RedisReminderTable(client, {
      ...toConfigOption("keyPrefix", options.keyPrefix),
      serviceId: this.serviceIdentity,
    });
    this.reminderServiceOptions = serviceOptions;
    return this;
  }

  /**
   * Enable durable reminders backed by Postgres. The connection pool is created
   * here, the reminder table is created on silo start (`start()`), and the pool
   * is closed on stop; `tableName` namespaces rows (defaults to `"thresh_reminders"`).
   */
  usePostgresReminders(options: { connectionString: string; tableName?: string }): this {
    const pool = new Pool({ connectionString: options.connectionString });
    pool.on("error", () => {});
    const table = new PostgresReminderTable(pool, {
      ...toConfigOption("tableName", options.tableName),
      serviceId: this.serviceIdentity,
    });
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
   * namespaces keys (defaults to `"thresh"`). `options` mirrors Orleans
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
  /**
   * `provider` lets a caller supply an existing `MemoryStreamProvider`
   * instance instead of constructing a fresh one — `TestCluster` uses this to
   * share one named provider cluster-wide (mirroring how it shares `storage`/
   * `reminderTable`), so a producer and consumer landing on different silos
   * still publish/subscribe into the same instance.
   */
  useMemoryStreams(
    name = "default",
    options?: { streamInactivityPeriodMs?: number },
    provider: MemoryStreamProvider = new MemoryStreamProvider(name),
  ): this {
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
   * Register a durable store for permanently-failed (poison) stream
   * deliveries (Orleans' `AzureTableStorageStreamFailureHandler` equivalent).
   * Every subsequent `addRedisStreams` call that doesn't supply its own
   * `failureHandler` gets one backed by this store, so poison events across
   * every Redis stream provider on this silo land in one place. Defaults to
   * an in-memory store; pass a durable one for a real deployment.
   */
  useDurableStreamFailureStore(
    store: DurableStreamFailureStore = new MemoryStreamFailureStore(),
  ): this {
    this.streamFailureStore = store;
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
   * (defaults to `"thresh"`). `options.failureHandler` (Orleans
   * `IStreamFailureHandler`) is forwarded to every queue's pulling agent,
   * defaulting to a handler backed by `useDurableStreamFailureStore`'s store
   * when one was registered and this call supplies none of its own.
   */
  addRedisStreams(
    name: string,
    options: { url: string; keyPrefix?: string; failureHandler?: StreamFailureHandler },
  ): this {
    const client = createClient({ url: options.url });
    client.on("error", () => {});
    const failureHandler =
      options.failureHandler ??
      (this.streamFailureStore !== undefined
        ? new DurableStreamFailureHandler(this.streamFailureStore)
        : undefined);
    const provider = new RedisPullingStreamProvider(client, name, {
      ...toConfigOption("keyPrefix", options.keyPrefix),
      ...toConfigOption("failureHandler", failureHandler),
      serviceId: this.serviceIdentity,
    });
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
   * Register a Postgres-backed stream provider (durable, cluster-shared) —
   * the Postgres counterpart to `addRedisStreams` for a deployment that would
   * otherwise need Redis solely for streams
   * (docs/stream-backings-postgres-kafka.md Phase 1). The connection pool is
   * created here, the backing tables (`<tablePrefix>_events`,
   * `<tablePrefix>_cursors`, `<tablePrefix>_subscriptions`) are provisioned
   * on silo start, and the pool is closed on stop; the provider's poll loops
   * are stopped on shutdown too. `tablePrefix` namespaces the tables
   * (defaults to `"thresh_stream"`). `options.failureHandler` (Orleans
   * `IStreamFailureHandler`) is forwarded to every queue's pulling agent,
   * defaulting to a handler backed by `useDurableStreamFailureStore`'s store
   * when one was registered and this call supplies none of its own.
   * `options.retainFor` keeps a replay window of delivered rows instead of
   * trimming them eagerly on commit.
   */
  addPostgresStreams(
    name: string,
    options: {
      connectionString: string;
      queueCount?: number;
      pollIntervalMs?: number;
      tablePrefix?: string;
      failureHandler?: StreamFailureHandler;
      retainFor?: Duration;
    },
  ): this {
    const pool = new Pool({ connectionString: options.connectionString });
    pool.on("error", () => {}); // surfaced by start()/queries; don't crash the process
    const failureHandler =
      options.failureHandler ??
      (this.streamFailureStore !== undefined
        ? new DurableStreamFailureHandler(this.streamFailureStore)
        : undefined);
    const provider = new PostgresPullingStreamProvider(pool, name, {
      ...toConfigOption("queueCount", options.queueCount),
      ...toConfigOption("pollIntervalMs", options.pollIntervalMs),
      ...toConfigOption("tablePrefix", options.tablePrefix),
      ...toConfigOption("failureHandler", failureHandler),
      ...toConfigOption("retainFor", options.retainFor),
      serviceId: this.serviceIdentity,
    });
    this.pullingStreams.push(provider);
    this.starters.push(async () => {
      await provider.start();
    });
    this.closers.push(async () => {
      provider.stop();
      await pool.end();
    });
    return this.addStreamProvider(name, provider);
  }

  /**
   * Register a Kafka-backed stream provider (durable, cluster-shared) —
   * teams with Kafka as their event backbone publish/consume streams without
   * a second durable system for the transport
   * (docs/stream-backings-postgres-kafka.md Phase 2). One topic per provider
   * (`<topicPrefix>.<name>`, default `"thresh.streams"`); one Kafka partition
   * per physical queue, validated to match `queueCount` when the silo
   * starts (topic auto-creation is not assumed — create the topic with the
   * desired partition count out of band).
   *
   * Kafka has no metadata surface for cursors/subscriptions/failures, so
   * `options.metadata` supplies a Postgres or Redis store for that trio —
   * exactly the stores `addPostgresStreams`/`addRedisStreams` already build
   * (`PostgresSubscriptionRegistry`+`PostgresStreamCursorStore`, or
   * `RedisSubscriptionRegistry`+`RedisStreamCursorStore`); Orleans' EventHubs
   * provider keeps its checkpoints in Azure Table storage the same way. The
   * connection (pool or client) is created and torn down here, alongside the
   * Kafka producer/consumer/admin client. `options.failureHandler` (Orleans
   * `IStreamFailureHandler`) is forwarded to every queue's pulling agent —
   * and to the retention-gap edge case — defaulting to a handler backed by
   * `useDurableStreamFailureStore`'s store when one was registered and this
   * call supplies none of its own.
   */
  addKafkaStreams(
    name: string,
    options: {
      brokers: string[];
      queueCount?: number;
      pollIntervalMs?: number;
      topicPrefix?: string;
      failureHandler?: StreamFailureHandler;
      metadata:
        | { postgres: { connectionString: string; tablePrefix?: string } }
        | { redis: { url: string; keyPrefix?: string } };
    },
  ): this {
    if (!Array.isArray(options.brokers) || options.brokers.length === 0) {
      throw new StreamProviderConfigurationError(name, "brokers must be a non-empty array");
    }
    const kafka = new Kafka({ clientId: `thresh-streams-${name}`, brokers: options.brokers });

    let registry: SubscriptionRegistry;
    let cursorStore: StreamCursorStore;
    if ("postgres" in options.metadata) {
      const { connectionString, tablePrefix } = options.metadata.postgres;
      const prefix = tablePrefix ?? "thresh_stream";
      const pool = new Pool({ connectionString });
      pool.on("error", () => {}); // surfaced by start()/queries; don't crash the process
      registry = new PostgresSubscriptionRegistry(pool, prefix, name, this.serviceIdentity);
      cursorStore = new PostgresStreamCursorStore(pool, prefix, this.serviceIdentity);
      this.closers.push(async () => {
        await pool.end();
      });
    } else {
      const { url, keyPrefix } = options.metadata.redis;
      const prefix = keyPrefix ?? "thresh";
      const client = createClient({ url });
      client.on("error", () => {});
      registry = new RedisSubscriptionRegistry(client, prefix, name, this.serviceIdentity);
      cursorStore = new RedisStreamCursorStore(client, prefix, this.serviceIdentity);
      this.starters.push(async () => {
        await client.connect();
      });
      this.closers.push(async () => {
        await client.close();
      });
    }

    const failureHandler =
      options.failureHandler ??
      (this.streamFailureStore !== undefined
        ? new DurableStreamFailureHandler(this.streamFailureStore)
        : undefined);
    const provider = new KafkaPullingStreamProvider(kafka, name, {
      registry,
      cursorStore,
      ...toConfigOption("queueCount", options.queueCount),
      ...toConfigOption("pollIntervalMs", options.pollIntervalMs),
      ...toConfigOption("topicPrefix", options.topicPrefix),
      ...toConfigOption("failureHandler", failureHandler),
    });
    this.pullingStreams.push(provider);
    this.starters.push(async () => {
      await provider.start();
    });
    this.closers.push(async () => {
      provider.stop();
      await provider.disconnect();
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

  /**
   * This silo's own address, as configured. A host that configures its own
   * membership view (a single-silo dev host naming itself in
   * `useStaticMembership`) would otherwise have to be handed the address a
   * second time, alongside the builder that already holds it.
   */
  get local(): SiloAddress {
    return this.config.local;
  }

  /** The cluster this silo joins, as configured. */
  get clusterId(): string {
    return this.config.clusterId;
  }

  /**
   * The storage provider registered under `name`, or `undefined` when nothing
   * is registered under it. The read-back that lets a host hand the SAME
   * provider instance to something outside the facet machinery — a grain that
   * takes its storage by constructor injection, say — including the instance a
   * convenience registration (`addPostgresStorage`, `addRedisStorage`) built
   * for itself and would otherwise keep private.
   */
  storageProvider(name: string): GrainStorage | undefined {
    return this.storage?.tryGet(name);
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
   * Register a named custom placement strategy (Orleans `IPlacementDirector` behind a
   * `[PlacementStrategy]` attribute). A grain opts in with
   * `{ placement: "custom", strategy: name }`, resolved here by `name` at placement time.
   * The whole-strategy counterpart of `addPlacementFilter`, which prunes candidates rather
   * than choosing among them.
   */
  addPlacementStrategy(name: string, strategy: PlacementStrategy): this {
    (this.placementStrategies ??= new PlacementStrategyRegistry()).add(name, strategy);
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
    this.logger = logger;
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
   * shared by several clusters (defaults to `"thresh"`), and this silo's
   * This silo's `serviceId` is carried in the key as well, so two services
   * sharing one prefix stay partitioned (Orleans' `{ServiceId}/state/`).
   */
  addRedisStorage(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = this.createManagedRedisClient(options);
    return this.addStorage(
      name,
      new RedisGrainStorage(client, {
        ...toConfigOption("keyPrefix", options.keyPrefix),
        serviceId: this.serviceIdentity,
      }),
    );
  }

  /**
   * Register a Postgres-backed storage provider. The connection pool is created
   * here, the backing table is created on silo start (`start()`), and the pool is
   * closed on stop; `tableName` namespaces rows in a Postgres shared by several
   * clusters (defaults to `"thresh_grain_state"`), and this silo's
   * this silo's `serviceId` is part of the row key as well, so two services
   * sharing one table stay partitioned (Orleans' `OrleansStorage.serviceid`).
   * Good when state must also be queried outside the actor model.
   */
  addPostgresStorage(
    name: string,
    options: { connectionString: string; tableName?: string },
  ): this {
    const pool = new Pool({ connectionString: options.connectionString });
    pool.on("error", () => {}); // surfaced by start()/queries; don't crash the process
    const provider = new PostgresGrainStorage(pool, {
      ...toConfigOption("tableName", options.tableName),
      serviceId: this.serviceIdentity,
    });
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
   * `keyPrefix` namespaces keys (defaults to `"thresh"`).
   */
  addRedisTransactionalStorage(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = this.createManagedRedisClient(options);
    return this.addTransactionalStorage(
      name,
      new RedisTransactionalStorage(client, {
        ...toConfigOption("keyPrefix", options.keyPrefix),
        serviceId: this.serviceIdentity,
      }),
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
   * `keyPrefix` namespaces keys (defaults to `"thresh"`).
   */
  addRedisJournaling(name: string, options: { url: string; keyPrefix?: string }): this {
    const client = this.createManagedRedisClient(options);
    return this.addJournaling(
      name,
      new RedisJournalStorage(client, {
        ...toConfigOption("keyPrefix", options.keyPrefix),
        serviceId: this.serviceIdentity,
      }),
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
   * drain. For role/metadata-aware placement, pass `options.metadataLabelPrefix` and
   * build the watch's client source with `fetchPodLabels: true` so pod labels reach
   * `SiloMember.metadata`.
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
    this.inProcessNetwork = network;
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

  /**
   * Register a grain type. A `defineGrain`/`defineReducerGrain` definition
   * carries its own interface, so it needs nothing else; a bare constructor
   * (the `@grain()` class form) must name the interfaces it answers to. An
   * explicit `interfaces` list is used verbatim — see `normalizeRegistration`.
   */
  registerGrain(definition: Registrable, registration?: GrainRegistration): this;
  registerGrain(ctor: GrainClass, registration: GrainRegistration): this;
  registerGrain(definition: Registrable | GrainClass, registration?: GrainRegistration): this {
    // Normalize once, here, so both replays in `build()` (the `ClusterNode`
    // and the embedded `ClientNode`) read the same resolved records.
    this.registrations.push(normalizeRegistration(definition, registration));
    return this;
  }

  registerGrains(registrations: readonly (Registrable | Registration)[]): this {
    for (const entry of registrations) {
      this.registrations.push(
        "ctor" in entry
          ? normalizeRegistration(entry.ctor, { interfaces: [...entry.interfaces] })
          : normalizeRegistration(entry),
      );
    }
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
   * Register a per-silo grain service (Orleans `ISiloBuilder.AddGrainService<TGrainService>()`):
   * one instance of `factory()`'s result is built for this silo at startup,
   * `init`'d then `start`'d before the silo is marked ready, and `stop`'d on
   * shutdown. Reachable from grain code via `GrainRuntime.getGrainService(name)`.
   * `name` identifies the service (Orleans resolves by `TGrainService`'s
   * `GrainType`); registering twice under the same name replaces the factory.
   */
  addGrainService(name: string, factory: () => GrainService): this {
    this.grainServiceFactories.set(name, factory);
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

  /**
   * Declare that this silo's startup tasks host observer objects
   * (`GrainFactoryAccess.createObjectReference`), so that a transport which
   * cannot back that seam is rejected at `build()` rather than at the first
   * call.
   *
   * There is no Orleans counterpart: `IGrainFactory.CreateObjectReference`
   * works on every Orleans silo, because an Orleans client leg is duplex over
   * its own outbound connection. Thresh's embedded client is a real
   * `ClientNode` that LISTENS on its own endpoint — the silo dials it back to
   * deliver a call — so it can only be gatewayed through an in-process
   * network today; see `docs/deviations.md`. Declaring the requirement is how
   * a silo that needs the seam finds that out at build, instead of shipping
   * green tests (where `TestCluster` always configures an in-process
   * transport) and failing on the first observer call in production.
   *
   * Purely additive: a silo that does not declare it behaves exactly as
   * before, including the lazy throw from `createObjectReference`.
   */
  requireObserverHosting(): this {
    this.observerHostingRequired = true;
    return this;
  }

  build(): SiloHost {
    if (this.membership === undefined) throw new Error("silo: no membership configured");
    if (this.transport === undefined) throw new Error("silo: no transport configured");
    if (this.observerHostingRequired && this.embeddedClientLeg() === undefined) {
      throw new Error(
        "silo: requireObserverHosting() was declared, but the configured transport cannot back " +
          "the observer seam. GrainFactoryAccess.createObjectReference is hosted by an embedded " +
          "ClientNode that needs a listening leg on this silo's own network; " +
          "useInProcessTransport(network) and useWebSocketTransport() both provide one. Drop " +
          "requireObserverHosting() if no startup task calls createObjectReference.",
      );
    }
    validateClassSpecificCollectionAges(this.config.classSpecificCollectionAgeSeconds);
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
    const grainServiceRegistry = new GrainServiceRegistry();
    for (const [name, factory] of this.grainServiceFactories) {
      grainServiceRegistry.register(name, factory());
    }
    if (this.grainServiceFactories.size > 0) {
      // `init`+`start` every registered service before the silo is marked
      // ready (Orleans: grain services start as part of silo startup, before
      // BecomeActive), and `stop` them on shutdown.
      this.starters.push(async () => {
        await grainServiceRegistry.start();
      });
      this.closers.push(async () => {
        grainServiceRegistry.stop();
      });
    }
    const membership = this.membership;

    const activationOptions: ActivationOptions = {
      maxEnqueuedRequestsSoftLimit:
        this.config.scheduling?.maxEnqueuedRequestsSoftLimit ??
        DEFAULT_MAX_ENQUEUED_REQUESTS_SOFT_LIMIT,
      maxEnqueuedRequestsHardLimit:
        this.config.scheduling?.maxEnqueuedRequestsHardLimit ??
        DEFAULT_MAX_ENQUEUED_REQUESTS_HARD_LIMIT,
      maxRequestProcessingTimeMs:
        this.config.scheduling?.maxRequestProcessingTime !== undefined
          ? durationToMs(this.config.scheduling.maxRequestProcessingTime)
          : DEFAULT_MAX_REQUEST_PROCESSING_TIME_MS,
      ...(this.config.deactivationTimeout !== false
        ? {
            deactivationTimeoutMs:
              this.config.deactivationTimeout !== undefined
                ? durationToMs(this.config.deactivationTimeout)
                : DEFAULT_DEACTIVATION_TIMEOUT_MS,
          }
        : {}),
      ...(this.logger !== undefined ? { logger: this.logger } : {}),
    };

    const node = new ClusterNode({
      local: this.config.local,
      clusterId: this.config.clusterId,
      membership: this.membership,
      transport: this.transport,
      ...(time !== undefined ? { time } : {}),
      ...(this.config.collectionAgeSeconds !== undefined
        ? { defaultCollectionAgeSeconds: this.config.collectionAgeSeconds }
        : {}),
      ...(this.config.classSpecificCollectionAgeSeconds !== undefined
        ? { classSpecificCollectionAgeSeconds: this.config.classSpecificCollectionAgeSeconds }
        : {}),
      ...(this.config.defaultPlacementStrategy !== undefined
        ? { defaultPlacementStrategy: this.config.defaultPlacementStrategy }
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
      activationOptions,
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
      ...(this.placementStrategies !== undefined
        ? { placementStrategyRegistry: this.placementStrategies }
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
          await bindTransactionalStates(
            instance,
            grainId,
            transactionalStorage,
            (manager, txId) => node.resolveTransactionStatus(manager, txId),
            time,
          );
        }
        if (grainFacets !== undefined) {
          await bindGrainFacets(instance, grainId, grainFacets);
        }
      },
      // Mirror image of `stateBinder`: stop a transactional-state facet's
      // confirmation-worker timer when its activation deactivates, so an idle
      // activation with an unresolved in-doubt record doesn't keep pinging its
      // TM forever (GAP: unbindTransactionalStates was previously never
      // called — see #23 follow-up). A no-op for an instance with no bound
      // facets (or when transactions are disabled), so this always runs.
      stateUnbinder: (instance) => unbindTransactionalStates(instance),
      ...(this.reminderTable !== undefined ? { reminderRegistry: () => reminderService } : {}),
      ...(this.jobShardStore !== undefined ? { durableJobScheduler: () => jobManager } : {}),
      ...(this.grainServiceFactories.size > 0
        ? { grainServiceRegistry: () => grainServiceRegistry }
        : {}),
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
        this.reminderServiceOptions ?? {},
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
        undefined,
        // Cross-silo scheduling: forward a job this silo persisted but doesn't
        // own the shard for to whichever active silo the membership view says
        // holds that ring key, so it still gets delivered.
        async (ownerRingKey, job) => {
          const target = activeSilos(membership.current()).find((s) => s.ringKey === ownerRingKey);
          if (target === undefined) return false;
          return node.forwardDurableJob(target, job);
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

    // Backs `GrainFactoryAccess.createObjectReference` for startup tasks:
    // built lazily (only if a startup task actually runs), reusing this
    // silo's own in-process network as its gateway. See `GrainFactoryAccess`.
    let embeddedClient: ClientNode | undefined;
    const local = this.config.local;
    const clusterId = this.config.clusterId;
    const registrations = this.registrations;
    const embeddedLeg = this.embeddedClientLeg();
    const closers = this.closers;
    const incomingCallFilters = this.incomingCallFilters;
    const outgoingCallFilters = this.outgoingCallFilters;
    const ensureEmbeddedClient = async (): Promise<void> => {
      // Needs a transport that can give the client a listening leg on this
      // silo's network — both of the transports the builder configures can, so
      // this only returns early for a silo built with neither.
      // `createObjectReference` below then throws lazily (only if actually
      // called) rather than failing every startup task, most of which never
      // touch the observer seam; `requireObserverHosting()` is the opt-in that
      // turns that into a build-time failure for a silo that does depend on it.
      if (embeddedClient !== undefined || embeddedLeg === undefined) return;
      const client = createClientNode({
        clusterId,
        local: embeddedLeg.address,
        transport: embeddedLeg.transport,
        gateway: local,
        // Symmetrical with the node's own filters (`ClusterNode`'s
        // `incomingCallFilters`/`outgoingCallFilters` above): a call INTO an
        // object this embedded client hosts (e.g. `useTracing()`'s SERVER
        // span via `tracingFilters().incoming`) and a call this client
        // issues itself both run the same filter pipeline the silo's own
        // grain calls do, so tracing (and any other registered filter)
        // covers `GrainFactoryAccess.createObjectReference` the same way it
        // covers ordinary grain calls.
        ...(incomingCallFilters.length > 0 ? { incomingCallFilters } : {}),
        ...(outgoingCallFilters.length > 0 ? { outgoingCallFilters } : {}),
      }).registerGrains(registrations);
      await client.connect();
      closers.push(async () => client.close());
      embeddedClient = client;
    };
    const grainFactoryAccess: GrainFactoryAccess = {
      getGrain: (def, key) => node.getGrain(def, key),
      createObjectReference: (def, obj) => {
        if (embeddedClient === undefined) {
          throw new Error(
            "GrainFactoryAccess.createObjectReference from a startup task requires a transport " +
              "that can give this silo's embedded client a listening leg; " +
              "useInProcessTransport(network) and useWebSocketTransport() both do. Declare " +
              "requireObserverHosting() on the builder to fail at build() instead of here, on " +
              "the first observer call.",
          );
        }
        return embeddedClient.createObjectReference(def, obj);
      },
      deleteObjectReference: (ref) => embeddedClient?.deleteObjectReference(ref),
    };

    return buildSiloHost({
      node,
      serviceId: this.serviceIdentity,
      health,
      healthServer: this.healthPort !== undefined ? new HealthServer(health) : undefined,
      healthPort: this.healthPort,
      membership: this.membership,
      reminderService,
      rebalancerWorker,
      onStart: this.starters,
      onOwnershipChange,
      onStop: this.closers,
      startupTasks: this.startupTasks.map((fn) => async () => {
        await ensureEmbeddedClient(); // no-op without an in-process network
        await fn(grainFactoryAccess);
      }),
    });
  }
}

/**
 * Reject a nonsense per-grain-type collection age at silo BUILD, the way Orleans'
 * `GrainCollectionOptionsValidator` rejects one at host start rather than letting a
 * bad age reach the collector. Only the "is this a duration at all" half is ported:
 * Orleans additionally requires every age to exceed `CollectionQuantum`, which this
 * runtime deliberately allows (see `docs/deviations.md`) — a sub-sweep age is honest
 * here, it simply collects on the first sweep at or after the age elapses.
 */
function validateClassSpecificCollectionAges(
  ages: Readonly<Record<string, number>> | undefined,
): void {
  if (ages === undefined) return;
  for (const [grainType, seconds] of Object.entries(ages)) {
    if (!Number.isFinite(seconds) || seconds <= 0) {
      throw new Error(
        `silo: classSpecificCollectionAgeSeconds["${grainType}"] is ${seconds}; ` +
          `a collection age must be a finite number of seconds greater than 0`,
      );
    }
  }
}

export function createSilo(config: SiloConfig): SiloBuilder {
  return new SiloBuilder(config);
}
