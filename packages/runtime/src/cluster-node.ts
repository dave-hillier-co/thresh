import { context, propagation } from "@opentelemetry/api";
import { newActivationId } from "@tsva/core/activation-id";
import { clientIdOf, isClient } from "@tsva/core/client-grain-id";
import { GrainCallError, RejectionError } from "@tsva/core/errors";
import type { Grain } from "@tsva/core/grain";
import { grainAddressEquals, type GrainAddress } from "@tsva/core/grain-address";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import { getGrainInterface } from "@tsva/core/grain-interface";
import type { InterfaceVersionEntry, SiloManifest } from "@tsva/core/grain-manifest";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import type { GrainType } from "@tsva/core/grain-type";
import {
  compatibilityDirector,
  type CompatibilityDirector,
  type CompatibilityKind,
} from "@tsva/core/version-compatibility";
import {
  versionSelector,
  type VersionSelectorKind,
  type VersionSelectorStrategy,
} from "@tsva/core/version-selector";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import { activeSilos, type MembershipService } from "@tsva/core/membership";
import type {
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
import { Guid } from "@tsva/core/guid";
import type { GrainReferenceIdentity } from "@tsva/core/grain-reference";
import { RemindableInterface, type ReminderRegistry, type TickStatus } from "@tsva/core/reminder";
import {
  DurableJobConsumerInterface,
  type DurableJobRunResult,
  type DurableJobScheduler,
  type JobRunContext,
} from "@tsva/core/durable-job";
import {
  BroadcastChannelPublisherInterface,
  BroadcastConsumerInterface,
  channelKey,
  type BroadcastChannelOptions,
  type BroadcastChannelProvider,
  type ChannelId,
} from "@tsva/core/broadcast-channel";
import { emitBroadcastChannelEvent } from "@tsva/core/broadcast-channel-diagnostics";
import {
  ConstructorChannelNamespacePredicateProvider,
  matchesChannelNamespace,
} from "@tsva/core/channel-namespace-predicate";
import { StreamConsumerInterface, type Controllable, type StreamProvider } from "@tsva/core/stream";
import type { InvocationRequest } from "@tsva/core/request";
import {
  IManagementGrain,
  type DetailedGrainStatistic,
  type SimpleGrainStatistic,
} from "@tsva/core/management-grain";
import type { SiloAddress } from "@tsva/core/silo-address";
import {
  participantKey,
  type ParticipantId,
  type TransactionInfo,
} from "@tsva/core/transaction-info";
import { TransactionResourceInterface } from "@tsva/core/transaction-resource";
import { ConsistentHashRing } from "@tsva/directory/consistent-hash-ring";
import type { DirectoryPeer } from "@tsva/directory/directory-peer";
import { DistributedGrainDirectory } from "@tsva/directory/distributed-grain-directory";
import type { GrainDirectory } from "@tsva/directory/grain-directory";
import { LocalDirectoryPartition } from "@tsva/directory/local-directory-partition";
import { LocationCache } from "@tsva/directory/location-cache";
import { ConnectionManager } from "@tsva/messaging/connection-manager";
import { CorrelationTable } from "@tsva/messaging/correlation-table";
import {
  nextCorrelationId,
  responseTo,
  type Message,
  type ResponseKind,
} from "@tsva/messaging/message";
import { MessagePackSerializer } from "@tsva/messaging/msgpack-serializer";
import type { Serializer } from "@tsva/messaging/serializer";
import type {
  Connection,
  ConnectionPreamble,
  Listener,
  Transport,
} from "@tsva/messaging/transport";
import { ActivationCollector } from "@tsva/runtime/activation-collector";
import { ICancellationSourcesExtension } from "@tsva/runtime/cancellation-extension";
import { Catalog, type GrainActivator, type RegisteredGrain } from "@tsva/runtime/catalog";
import {
  withFilterPlacementCandidatesSpan,
  withPlaceGrainSpan,
} from "@tsva/observability/activation-tracing";
import { ClientDirectory } from "@tsva/runtime/client-directory";
import type { ActivationData, ActivationOptions } from "@tsva/runtime/activation";
import { DistributedDispatcher } from "@tsva/runtime/distributed-dispatcher";
import { BroadcastChannelProviderImpl } from "@tsva/runtime/broadcast-channel-provider";
import { GrainFactory } from "@tsva/runtime/grain-factory";
import type { GrainServiceRegistry } from "@tsva/runtime/grain-service";
import { createManagementGrainType } from "@tsva/runtime/management-grain";
import { chooseMigrationTarget } from "@tsva/runtime/placement/choose-migration-target";
import {
  placementFiltersFor,
  placementStrategyFor,
} from "@tsva/runtime/placement/placement-director";
import { resolvePlacementHintValue } from "@tsva/runtime/placement/placement-hint";
import type { PlacementFilter } from "@tsva/runtime/placement/placement-filter";
import type { PlacementFilterRegistry } from "@tsva/runtime/placement/placement-filter-registry";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";
import { filterByVersion } from "@tsva/runtime/placement/version-placement-filter";
import {
  ActivationRepartitioner,
  rebalancerCompatibleTolerance,
  type AcceptExchangeRequest,
  type AcceptExchangeResponse,
  type ImbalanceToleranceRule,
} from "@tsva/runtime/placement/repartitioning/activation-repartitioner";
import type { ActivationRepartitionerOptions } from "@tsva/runtime/placement/repartitioning/activation-repartitioner-options";
import type { Edge } from "@tsva/runtime/placement/repartitioning/edge";
import {
  DEFAULT_REBALANCER_OPTIONS,
  planCycle,
  type CycleState,
  type RebalancerOptions,
  type StopReason,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import { systemTimeProvider, type TimeProvider } from "@tsva/runtime/time-provider";
import { TransactionAgent } from "@tsva/runtime/transaction-agent";
import {
  DEFAULT_LOAD_SHEDDING_OPTIONS,
  OverloadDetector,
  TestHooksEnvironmentStatisticsProvider,
  type LoadSheddingOptions,
  type SiloLoadSheddingTestHooks,
} from "@tsva/runtime/load-shedding";
import { GatewayTooBusyException } from "@tsva/core/errors";
import { executeWithRetries, fixedBackoff } from "@tsva/core/async-executor-with-retries";

/**
 * Recovery-pull tuning (Orleans' directory partition recovery has no direct
 * equivalent knob; these are this port's own bounds). `maxAttempts` bounds
 * `executeWithRetries` per source silo; `retentionMs` bounds how long a
 * handed-off entry is retained for a successor that never pulls it (its
 * host silo crashed mid-handoff, or the puller crashed after pulling but
 * before ACKing) — past that it is dropped so the source's memory does not
 * grow unbounded and a very late, stale pull is not served.
 */
const DEFAULT_RECOVERY_MAX_ATTEMPTS = 3;
const DEFAULT_RECOVERY_BACKOFF_MS = 200;
const DEFAULT_RECOVERY_RETENTION_MS = 60_000;

export interface ClusterNodeOptions {
  local: SiloAddress;
  clusterId: string;
  membership: MembershipService;
  transport: Transport;
  /** Reaches peer directory partitions; defaults to routing over the transport. */
  directoryPeer?: DirectoryPeer;
  serializer?: Serializer;
  time?: TimeProvider;
  callTimeoutMs?: number;
  /** Tuning for join/handoff directory-range recovery pulls; see the module-level defaults. */
  recovery?: {
    maxAttempts?: number;
    backoffMs?: number;
    retentionMs?: number;
  };
  /**
   * Silo-wide default per-method response timeout (ms), applied to any call
   * whose interface doesn't set its own `options.responseTimeout` (Orleans
   * `[ResponseTimeout]`). Off by default (`undefined`): no deadline races the
   * call unless one is explicitly configured, here or per-method.
   */
  defaultResponseTimeoutMs?: number;
  defaultCollectionAgeSeconds?: number;
  /** How often the idle-collection sweep runs (defaults to 60s). */
  collectionIntervalSeconds?: number;
  /**
   * Bind state before `onActivate` (provided by the hosting layer). `"rehydrate"`
   * mode binds facets without reading storage so migrated state is preserved.
   */
  stateBinder?: (
    instance: Grain,
    grainId: GrainId,
    mode?: "activate" | "rehydrate",
  ) => Promise<void>;
  /** Resolves the reminder registry a grain's `registerReminder` delegates to. */
  reminderRegistry?: () => ReminderRegistry | undefined;
  /** Resolves the durable-job scheduler a grain's `scheduleJob` delegates to. */
  durableJobScheduler?: () => DurableJobScheduler | undefined;
  /** Resolves this silo's grain-service registry, for a grain's `runtime.getGrainService()`. */
  grainServiceRegistry?: () => GrainServiceRegistry | undefined;
  /** Resolves the stream provider a grain's `getStreamProvider` returns. */
  streamProvider?: (name?: string) => StreamProvider | undefined;
  /**
   * Broadcast-channel providers configured on this silo. Each becomes a
   * `getBroadcastChannelProvider(name)` whose writers fan out to the
   * channel's implicit subscribers. The first registered name is the
   * default. A plain string is shorthand for `{ name, fireAndForgetDelivery:
   * true }` (Orleans' `BroadcastChannelOptions.FireAndForgetDelivery` default).
   */
  broadcastProviders?: readonly (string | ({ name: string } & BroadcastChannelOptions))[];
  /** Incoming grain-call filters wrapping each grain-method dispatch (silo-wide). */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
  /**
   * Dev-mode `@readOnly` mutation guard (`GAP-READONLY-ENFORCEMENT`): when
   * `true`, a `readOnly` turn's `@persistentState` fields are proxy-guarded
   * for the turn's duration, rejecting a mutation attempt with
   * `ReadOnlyStateViolationError` instead of silently succeeding. Opt-in and
   * `false` by default — zero overhead unless a silo turns it on.
   */
  readOnlyStateGuard?: boolean;
  /** Auto-install factories for `GrainExtension` interfaces, keyed by interface id (Orleans `AddGrainExtension`). */
  grainExtensionFactories?: ReadonlyMap<number, () => object>;
  /** Outgoing grain-call filters wrapping each outbound call at the proxy (silo-wide). */
  outgoingCallFilters?: readonly OutgoingGrainCallFilter[];
  /** Injectable RNG for deterministic placement in tests. */
  random?: () => number;
  /**
   * Grain-interface versioning policy. Setting either field enables
   * version-aware placement on this silo; otherwise versioning activates only
   * when a registered interface declares a version > 1. Defaults:
   * backward-compatible director + latest-version selector.
   */
  versionCompatibility?: CompatibilityKind;
  versionSelector?: VersionSelectorKind;
  /** Static metadata the local silo advertises (e.g. `{ role: "worker" }`), for metadata-aware placement. */
  metadata?: Readonly<Record<string, string>>;
  /**
   * Whether this silo has transaction support configured. Defaults to `true`.
   * When `false`, no `TransactionAgent` is wired, so any `[Transaction]`-style
   * grain call (a method whose `TransactionOption` isn't
   * `"suppress"`/`"notAllowed"`/`undefined`) throws
   * `TransactionsDisabledError` (Orleans parity: transactions are opt-in).
   */
  transactionsEnabled?: boolean;
  /**
   * Optional hook to construct/dispose grain instances instead of `new ctor()` —
   * e.g. object pooling or non-DI construction. Defaults to `new ctor()` when unset.
   */
  grainActivator?: GrainActivator;
  /**
   * Named custom placement-filter directors a grain's `"custom"`
   * `placementFilters` descriptor resolves against (Orleans `AddPlacementFilter`).
   */
  placementFilterRegistry?: PlacementFilterRegistry;
  /**
   * Load-shedding config (Orleans `LoadSheddingOptions`). Defaults to shedding
   * disabled. When enabled and this silo's (test-hooks-latched) CPU usage
   * exceeds `cpuThreshold`, a client-originated request arriving at this silo
   * as gateway is rejected with a `"overloaded"` `RejectionError`
   * (`GatewayTooBusyException` client-side) instead of being dispatched.
   */
  loadShedding?: Partial<LoadSheddingOptions>;
  /**
   * Enables the activation repartitioner (Orleans `AddActivationRepartitioner`):
   * a communication-graph-driven protocol that moves grain activations near
   * their most frequent communication partners. Only when set does the
   * dispatcher record per-call communication edges (see
   * `ActivationRepartitioner`'s doc for this port's scope) — every other silo
   * pays no cost for this subsystem at all.
   */
  repartitioning?: {
    options: ActivationRepartitionerOptions;
    /** Defaults to `rebalancerCompatibleTolerance(membership)`. */
    toleranceRule?: ImbalanceToleranceRule;
  };
  /**
   * Turn-scheduler back-pressure/watchdog and deactivation-timeout knobs
   * (Orleans `SchedulingOptions` + `CollectionOptions.DeactivationTimeout`),
   * applied to every activation on this silo. Unset (the default) keeps
   * every activation's queue unbounded and its deactivation untimed; see
   * `SiloBuilder`, which turns these on with Orleans-like defaults.
   */
  activationOptions?: ActivationOptions;
}

interface RejectionPayload {
  message: string;
  kind: RejectionError["kind"];
}

/**
 * A directory partition operation routed to the owning silo over the transport.
 * Every op carries the sender's applied membership view `version` so the owner
 * can linearise it against its own view (catch up if behind, redirect a stale
 * caller). `recover` pulls a previous owner's handed-off entries on a join.
 */
type DirectoryOp =
  | { kind: "lookup"; grainId: GrainId; version: number }
  | { kind: "register"; addr: GrainAddress; previous?: GrainAddress | undefined; version: number }
  | { kind: "unregister"; addr: GrainAddress; version: number }
  | { kind: "recover"; version: number }
  /** Puller's ACK that it applied a recovery batch: the source deletes exactly those served entries. */
  | { kind: "recoverAck"; grainIds: GrainId[]; version: number };

/** Stand-in target grain for batch ops with no single grain (fills the envelope only). */
const DIRECTORY_OP_TARGET = new GrainId("$directory", "op");

function directoryOpGrainId(op: DirectoryOp): GrainId {
  if (op.kind === "lookup") return op.grainId;
  if (op.kind === "recover" || op.kind === "recoverAck") return DIRECTORY_OP_TARGET;
  return op.addr.grainId;
}

/** Carries a migrating activation's identity and dehydrated state to its new host. */
interface MigrationPayload {
  grainId: GrainId;
  sourceAddr: GrainAddress;
  bag: Record<string, unknown>;
}

/**
 * A gossiped update to the cluster-wide client directory: `register` on
 * accepting a client connection, `unregister` on it disconnecting. Sent
 * `oneWay` to every other active silo so each silo's `ClientDirectory` learns
 * which gateway(s) a client is reachable through (GrainId/SiloAddress round-trip
 * through the serializer as class instances via the value codec's tagging, so
 * no manual (de)serialization to primitives is needed here).
 */
interface ClientGossip {
  op: "register" | "unregister";
  clientId: GrainId;
  gateway: SiloAddress;
}

const newChainId = () => Guid.newGuid().toString();
const randomPlacement = new RandomPlacement();

/**
 * A silo participating in a multi-silo cluster: it owns a directory partition,
 * routes grain calls via the distributed dispatcher, and exchanges request /
 * response messages with peers over the transport.
 */
export class ClusterNode {
  readonly partition: LocalDirectoryPartition;

  /** This silo's activation-repartitioner protocol object; `undefined` unless `repartitioning` was configured. */
  private readonly repartitioner?: ActivationRepartitioner;
  private readonly grainTypes = new Map<GrainType, RegisteredGrain>();
  private readonly interfaceToGrainType = new Map<number, GrainType>();
  /** Stream namespace → grain types implicitly subscribed to it (auto-subscribe by key). */
  private readonly implicitSubscriptions = new Map<string, GrainType[]>();
  /**
   * Broadcast-channel namespace → grain types implicitly subscribed to it via
   * an EXACT namespace match (`@implicitChannelSubscription`).
   */
  private readonly broadcastSubscriptions = new Map<string, GrainType[]>();
  /**
   * Broadcast-channel PREDICATE subscriptions (`@regexImplicitChannelSubscription`
   * and any other `"ctor:..."`-pattern predicate) — checked against a
   * published channel's namespace IN ADDITION to the exact-match map above,
   * since a grain may match by predicate without ever appearing as an exact
   * key.
   */
  private readonly broadcastPredicateSubscriptions: Array<{ pattern: string; type: GrainType }> =
    [];
  /** Configured broadcast-channel provider names; the first is the default. */
  private readonly broadcastProviderNames: readonly string[];
  /**
   * Per-provider fire-and-forget delivery flag (Orleans
   * `BroadcastChannelOptions.FireAndForgetDelivery`). Defaults to `false`
   * here (Orleans defaults to `true`) — see the constructor.
   */
  private readonly broadcastFireAndForget = new Map<string, boolean>();
  /** Interface versions this silo implements: interfaceId -> hosted version. */
  private readonly localVersions = new Map<number, { version: number; grainType: GrainType }>();
  private maxLocalVersion = 1;
  /** Peer manifests, fetched lazily and cleared on any membership change. */
  private readonly manifestCache = new Map<string, SiloManifest>();
  private readonly manifestInflight = new Map<string, Promise<SiloManifest>>();
  private readonly versionPolicyConfigured: boolean;
  private readonly director: CompatibilityDirector;
  private readonly selector: VersionSelectorStrategy;
  private readonly cache = new LocationCache();
  private readonly correlation = new CorrelationTable();
  private readonly connections: ConnectionManager;
  private readonly factory: GrainFactory;
  private readonly serializer: Serializer;
  private readonly catalog: Catalog;
  private readonly collector: ActivationCollector;
  private readonly dispatcher: DistributedDispatcher;
  private readonly directory: DistributedGrainDirectory;
  private readonly callTimeoutMs: number;
  /** Per-silo view of which gateway(s) each connected client is reachable through, kept current by gossip. */
  private readonly clientDirectory = new ClientDirectory();
  /** Connections held open to clients accepted on this silo, keyed by `clientId.toString()`. */
  private readonly clientConnections = new Map<string, Connection>();
  /** This silo's (test-hooks-latched) CPU-usage source (Orleans `TestHooksEnvironmentStatisticsProvider`). */
  private readonly environmentStatistics = new TestHooksEnvironmentStatisticsProvider();
  /** Decides whether this silo is currently shedding load (Orleans `OverloadDetector`). */
  private readonly overloadDetector: OverloadDetector;
  /**
   * A peer's last-pushed load snapshot, keyed by ring key (Orleans
   * `DeploymentLoadPublisher`'s subscriber cache, fed by
   * `SiloStatisticsChangeNotification`). There is no periodic gossip timer
   * here — a snapshot lands only when `publishLoadStats` pushes one, which
   * `siloTestHooks()` does synchronously after every latch/unlatch (mirrors
   * Orleans' test-only `PropagateStatisticsToCluster`, which forces an
   * immediate `ForceRuntimeStatisticsCollection` rather than waiting for the
   * next periodic interval).
   */
  private readonly remoteLoadStats = new Map<
    string,
    { activationCount: number; isOverloaded: boolean }
  >();
  private ring: ConsistentHashRing;
  private listener: Listener | undefined;

  /** The membership view version `this.ring` was built from. */
  private appliedVersion: number;
  /**
   * Entries this silo handed off, retained for a successor to pull, keyed by
   * grain id. Populated (merged, not replaced — a later unrelated view change
   * must not silently lose an as-yet-unpulled entry) on `updateView`; an entry
   * is removed the moment its puller ACKs it, or once it expires (`retentionMs`
   * past `producedAt`, or its silo falls out of the live view) — see `pruneHandoffSnapshot`.
   */
  private readonly handoffSnapshot = new Map<string, { entry: GrainAddress; producedAt: number }>();
  /** In-flight range recovery after a join; owned reads wait on it so none miss. */
  private recovery: Promise<void> | undefined;
  /** Callers awaiting `this.appliedVersion` to reach a given version. */
  private viewWaiters: Array<{ version: number; resolve: () => void }> = [];
  private readonly time: TimeProvider;
  private readonly recoveryMaxAttempts: number;
  private readonly recoveryBackoffMs: number;
  private readonly recoveryRetentionMs: number;

  /** Routes directory operations to the owning silo's partition over the transport. */
  private readonly transportPeer: DirectoryPeer = {
    lookup: async (owner, grainId) => {
      const r = await this.sendDirectory(owner, {
        kind: "lookup",
        grainId,
        version: this.appliedVersion,
      });
      return r == null ? undefined : (r as GrainAddress);
    },
    register: async (owner, addr, previous) =>
      (await this.sendDirectory(owner, {
        kind: "register",
        addr,
        previous,
        version: this.appliedVersion,
      })) as GrainAddress,
    unregister: async (owner, addr) => {
      await this.sendDirectory(owner, { kind: "unregister", addr, version: this.appliedVersion });
    },
  };

  constructor(private readonly options: ClusterNodeOptions) {
    const time = options.time ?? systemTimeProvider;
    this.time = time;
    this.recoveryMaxAttempts = options.recovery?.maxAttempts ?? DEFAULT_RECOVERY_MAX_ATTEMPTS;
    this.recoveryBackoffMs = options.recovery?.backoffMs ?? DEFAULT_RECOVERY_BACKOFF_MS;
    this.recoveryRetentionMs = options.recovery?.retentionMs ?? DEFAULT_RECOVERY_RETENTION_MS;
    this.overloadDetector = new OverloadDetector(this.environmentStatistics, {
      ...DEFAULT_LOAD_SHEDDING_OPTIONS,
      ...options.loadShedding,
    });
    const broadcastConfigs = (options.broadcastProviders ?? []).map((p) =>
      typeof p === "string" ? { name: p } : p,
    );
    this.broadcastProviderNames = broadcastConfigs.map((c) => c.name);
    for (const c of broadcastConfigs) {
      // Unlike Orleans (default `true`), an unconfigured provider here
      // defaults to NON-fire-and-forget: this framework's broadcast channels
      // predate `fireAndForgetDelivery` and always awaited every subscriber
      // for error visibility (see `publishToBroadcastChannel`) — preserved as
      // the default so every existing caller keeps that behaviour; opt into
      // Orleans' fire-and-forget default per provider with `{
      // fireAndForgetDelivery: true }`.
      this.broadcastFireAndForget.set(c.name, c.fireAndForgetDelivery ?? false);
    }
    this.callTimeoutMs = options.callTimeoutMs ?? 30_000;
    this.versionPolicyConfigured =
      options.versionCompatibility !== undefined || options.versionSelector !== undefined;
    this.director = compatibilityDirector(options.versionCompatibility ?? "backwardCompatible");
    this.selector = versionSelector(options.versionSelector ?? "latest");
    this.ring = this.buildRing();
    this.appliedVersion = options.membership.current().version;
    // Orleans IsSiloDead: a directory entry whose host has fallen out of the
    // live membership view is treated as a miss on lookup, not returned as a
    // stale pointer. The partition consults the snapshot each call so it tracks
    // membership changes without needing explicit reconciliation just for reads.
    this.partition = new LocalDirectoryPartition((silo) =>
      activeSilos(options.membership.current()).some((s) => s.equals(silo)),
    );
    this.connections = new ConnectionManager(options.transport, options.local, options.clusterId);
    this.factory = new GrainFactory(
      (interfaceId) => this.resolveGrainType(interfaceId),
      time,
      options.defaultResponseTimeoutMs,
    );
    this.serializer =
      options.serializer ??
      new MessagePackSerializer({ resolveGrainReference: (id) => this.rehydrate(id) });
    this.directory = new DistributedGrainDirectory(
      options.local,
      this.partition,
      () => this.ring,
      options.directoryPeer ?? this.transportPeer,
      () => this.updateView(), // refresh on a stale-view rejection, then re-resolve
      (grainId) => this.awaitRecovered(grainId), // gate owned reads on range recovery
    );
    this.catalog = new Catalog({
      grainTypes: this.grainTypes,
      factory: this.factory,
      time,
      defaultCollectionAgeSeconds: options.defaultCollectionAgeSeconds ?? 900,
      onDeactivated: (a) => this.onDeactivated(a),
      migrate: (a) => this.migrateActivation(a),
      localSilo: () => this.options.local,
      // Cascade a cancellation to a token's forwarded target wherever it
      // lives, cluster-wide, via the same extension-reference substrate
      // `getExtensionReference` exposes for a caller's own cascade canceller.
      cancellationCanceller: (target, tokenId) =>
        this.getExtensionReference(ICancellationSourcesExtension, target).cancelRemoteToken(
          tokenId,
        ),
      ...(options.stateBinder !== undefined ? { activateState: options.stateBinder } : {}),
      ...(options.reminderRegistry !== undefined
        ? { reminderRegistry: options.reminderRegistry }
        : {}),
      ...(options.durableJobScheduler !== undefined
        ? { durableJobScheduler: options.durableJobScheduler }
        : {}),
      ...(options.grainServiceRegistry !== undefined
        ? { grainServiceRegistry: options.grainServiceRegistry }
        : {}),
      ...(options.streamProvider !== undefined ? { streamProvider: options.streamProvider } : {}),
      ...(this.broadcastProviderNames.length > 0
        ? { broadcastProvider: (name?: string) => this.broadcastChannelProvider(name) }
        : {}),
      ...(options.incomingCallFilters !== undefined
        ? { incomingCallFilters: options.incomingCallFilters }
        : {}),
      ...(options.readOnlyStateGuard !== undefined
        ? { readOnlyStateGuard: options.readOnlyStateGuard }
        : {}),
      ...(options.grainActivator !== undefined ? { grainActivator: options.grainActivator } : {}),
      ...(options.grainExtensionFactories !== undefined
        ? { grainExtensionFactories: options.grainExtensionFactories }
        : {}),
      ...(options.activationOptions !== undefined
        ? { activationOptions: options.activationOptions }
        : {}),
      loadShedding: () => this.siloTestHooks(),
      siloPing: (target, message) => this.pingSilo(target, message),
    });
    this.dispatcher = new DistributedDispatcher({
      local: options.local,
      directory: this.directory,
      cache: this.cache,
      catalog: this.catalog,
      remote: { send: (silo, req) => this.sendRemote(silo, req) },
      activeSilos: () => activeSilos(this.options.membership.current()),
      placementFor: (grainType) => this.placementFor(grainType),
      filtersFor: (grainType) => this.filtersFor(grainType),
      placementContext: () => this.placementContext(),
      versionFilter: (req, candidates) => this.applyVersionFilter(req, candidates),
      clientRouter: {
        isClientTarget: (t) => isClient(t),
        route: (req) => this.routeToClient(req),
      },
      ...(options.repartitioning !== undefined
        ? {
            recordEdge: (req, targetSilo) =>
              this.repartitioner?.recordLocalEdge(req.callingGrain, req.target, targetSilo),
          }
        : {}),
    });
    this.factory.setDispatcher(this.dispatcher);
    if (options.repartitioning !== undefined) {
      const repartitioning = options.repartitioning;
      this.repartitioner = new ActivationRepartitioner({
        local: options.local,
        membership: options.membership,
        options: repartitioning.options,
        toleranceRule:
          repartitioning.toleranceRule ?? rebalancerCompatibleTolerance(options.membership),
        activationCount: () => this.catalog.count(),
        isMigratable: (grainId) => this.isMigratableBy(grainId, "repartitioner"),
        migrate: (grainId, target) => this.migrateGrainToSilo(grainId, target),
        sendAcceptExchangeRequest: (target, request) =>
          this.sendAcceptExchangeRequest(target, request),
      });
    }
    if (options.transactionsEnabled ?? true) {
      const transactionAgent = new TransactionAgent(time);
      transactionAgent.setDispatcher(this.dispatcher);
      this.factory.setTransactionAgent(transactionAgent);
    }
    if (options.outgoingCallFilters !== undefined) {
      this.factory.setOutgoingCallFilters(options.outgoingCallFilters);
    }
    this.collector = new ActivationCollector(
      this.catalog,
      time,
      (options.collectionIntervalSeconds ?? 60) * 1000,
    );
    // Built-in system grain (Orleans `IManagementGrain`), registered on every
    // silo so `getGrain(IManagementGrain, 0n)` resolves wherever placement
    // lands it. Its class closes over THIS silo's hooks (membership,
    // cross-silo stats gathering, directory lookup) rather than reaching them
    // through the general `GrainRuntime`/`GrainActivator` surface, since it
    // needs no other silo-provided facet.
    this.registerGrain(
      createManagementGrainType({
        membershipSnapshot: () => this.options.membership.current(),
        gatherGrainStats: () => this.gatherClusterGrainStats(),
        gatherDetailedGrainStats: (types) => this.gatherClusterDetailedGrainStatistics(types),
        lookupActivation: (id) => this.directory.lookup(id),
        isStatelessWorker: (type) => this.grainTypes.get(type)?.metadata.options.stateless === true,
        activationCountFor: (id) => this.gatherGrainActivationCount(id),
        forceActivationCollection: (ageLimitMs) =>
          this.forceActivationCollectionCluster(ageLimitMs),
        sendControlCommandToProvider: (providerName, command, arg) =>
          this.sendControlCommandToProviderCluster(providerName, command, arg),
      }),
      { interfaces: [IManagementGrain] },
    );
  }

  registerGrain<G extends Grain>(
    ctor: new () => G,
    registration: { interfaces: GrainInterface<unknown>[] },
  ): this {
    const metadata = getGrainMetadata(ctor);
    if (metadata === undefined) throw new Error(`${ctor.name} is not decorated with @grain()`);
    this.grainTypes.set(metadata.grainType, { ctor, metadata });
    for (const iface of registration.interfaces) {
      this.interfaceToGrainType.set(iface.id, metadata.grainType);
      const existing = this.localVersions.get(iface.id);
      if (existing === undefined || iface.version > existing.version) {
        this.localVersions.set(iface.id, { version: iface.version, grainType: metadata.grainType });
      }
      if (iface.version > this.maxLocalVersion) this.maxLocalVersion = iface.version;
    }
    for (const namespace of metadata.implicitSubscriptions) {
      const types = this.implicitSubscriptions.get(namespace) ?? [];
      if (!types.includes(metadata.grainType)) types.push(metadata.grainType);
      this.implicitSubscriptions.set(namespace, types);
    }
    for (const pattern of metadata.broadcastSubscriptions) {
      // A "ctor:..."-prefixed pattern (e.g. `@regexImplicitChannelSubscription`)
      // is a PREDICATE, matched against each publish's namespace at delivery
      // time (see `broadcastGrainTypes`) — it can't be exact-keyed up front
      // since it may match many namespaces, including ones not yet seen. A
      // plain namespace string is the existing exact-match case.
      if (pattern.startsWith(`${ConstructorChannelNamespacePredicateProvider.prefix}:`)) {
        this.broadcastPredicateSubscriptions.push({ pattern, type: metadata.grainType });
        continue;
      }
      const types = this.broadcastSubscriptions.get(pattern) ?? [];
      if (!types.includes(metadata.grainType)) types.push(metadata.grainType);
      this.broadcastSubscriptions.set(pattern, types);
    }
    return this;
  }

  /**
   * This silo's load-shedding test hooks (Orleans test-only access to
   * `TestHooksEnvironmentStatisticsProvider`/`OverloadDetector` via the silo's
   * `IServiceProvider`): latch/unlatch the reported CPU usage or force the
   * overloaded state outright, and toggle overload detection on/off. Used both
   * directly by tests (`SiloHost.loadShedding`) and by `IPlacementTestGrain`'s
   * `enableOverloadDetection`/`latchCpuUsage`/`latchOverloaded`-style methods
   * (via `GrainRuntime`, see `grain-runtime-impl.ts`).
   */
  siloTestHooks(): SiloLoadSheddingTestHooks {
    return {
      enableOverloadDetection: (enabled) => {
        this.overloadDetector.enabled = enabled;
      },
      latchCpuUsage: (value) => {
        this.environmentStatistics.latchCpuUsage(value);
        return this.publishLoadStats();
      },
      unlatchCpuUsage: () => {
        this.environmentStatistics.unlatchHardwareStatistics();
        return this.publishLoadStats();
      },
      // "Overloaded" is just a CPU reading above the configured threshold —
      // there is no separate boolean the detector consults (Orleans mirrors
      // this: `LatchOverloaded` also just latches CPU to `cpuThreshold + 1`).
      latchOverloaded: () => {
        this.environmentStatistics.latchCpuUsage(this.loadSheddingCpuThreshold() + 1);
        return this.publishLoadStats();
      },
      unlatchOverloaded: () => {
        this.environmentStatistics.unlatchHardwareStatistics();
        return this.publishLoadStats();
      },
    };
  }

  private loadSheddingCpuThreshold(): number {
    return this.options.loadShedding?.cpuThreshold ?? DEFAULT_LOAD_SHEDDING_OPTIONS.cpuThreshold;
  }

  /** Grain types implicitly subscribed to a stream namespace (drives stream fan-out). */
  implicitGrainTypes(namespace: string): readonly GrainType[] {
    return this.implicitSubscriptions.get(namespace) ?? [];
  }

  /**
   * The named stream provider this silo hosts (Orleans `IClusterClient.GetStreamProvider`,
   * adapted: this framework has no separate client-process gateway to fetch a
   * stream-provider proxy through, so a caller reaches it via the silo directly —
   * `SiloHost.getStreamProvider`). `undefined` if no provider by that name is
   * configured, or none at all when the name is omitted.
   */
  streamProvider(name?: string): StreamProvider | undefined {
    return this.options.streamProvider?.(name);
  }

  /**
   * Grain types implicitly subscribed to a broadcast-channel namespace: every
   * exact-match subscriber PLUS every predicate subscriber whose pattern
   * matches `namespace` (e.g. a `@regexImplicitChannelSubscription`) —
   * deduplicated, since a grain type could in principle appear in both.
   */
  broadcastGrainTypes(namespace: string): readonly GrainType[] {
    const exact = this.broadcastSubscriptions.get(namespace) ?? [];
    const predicated = this.broadcastPredicateSubscriptions
      .filter((s) => matchesChannelNamespace(s.pattern, namespace))
      .map((s) => s.type);
    if (predicated.length === 0) return exact;
    return [...new Set([...exact, ...predicated])];
  }

  /** The named broadcast-channel provider, or `undefined` if none is configured under that name. */
  broadcastChannelProvider(name?: string): BroadcastChannelProvider | undefined {
    const resolved = name ?? this.broadcastProviderNames[0];
    if (resolved === undefined || !this.broadcastProviderNames.includes(resolved)) return undefined;
    return new BroadcastChannelProviderImpl(resolved, (provider, channel, item) =>
      this.publishToBroadcastChannel(provider, channel, item),
    );
  }

  /**
   * Publish an item to every grain implicitly subscribed to the channel's
   * namespace, each addressed by the channel key — routed through the dispatcher
   * as a `BroadcastConsumer` system call (directory → placement), reactivating an
   * idle subscriber, exactly like stream delivery. Emits an `itemPublished`
   * diagnostic event once, and an `itemDelivered` event per successful
   * subscriber delivery (`@tsva/core/broadcast-channel-diagnostics`) — tests
   * synchronize on these instead of racing the fan-out.
   *
   * Delivery-failure semantics follow the named provider's
   * `fireAndForgetDelivery` (default `false` here, unlike Orleans' `true` —
   * see the constructor):
   *  - fire-and-forget: `publish` doesn't wait for delivery at all, and a
   *    subscriber's throw is swallowed — visible only via the diagnostic
   *    (a failed delivery never emits `itemDelivered`) and the subscriber's
   *    own state.
   *  - non-fire-and-forget: every subscriber is awaited, and only once ALL
   *    have been tried are their failures aggregated into one thrown
   *    `AggregateError` (Orleans' `AggregateException`) — one failing
   *    subscriber never prevents the others from being tried.
   */
  async publishToBroadcastChannel(
    provider: string,
    channel: ChannelId,
    item: unknown,
  ): Promise<void> {
    const key = channelKey(channel);
    emitBroadcastChannelEvent({
      kind: "itemPublished",
      providerName: provider,
      channelId: channel,
    });
    const deliver = (type: GrainType): Promise<void> =>
      this.dispatcher
        .invoke({
          target: new GrainId(type, channel.key),
          interfaceId: BroadcastConsumerInterface.id,
          method: "onPublished",
          args: [key, item],
          options: {},
          reentrancyId: newChainId(),
        })
        .then(() => {
          emitBroadcastChannelEvent({
            kind: "itemDelivered",
            providerName: provider,
            channelId: channel,
          });
        });
    const deliveries = this.broadcastGrainTypes(channel.namespace).map(deliver);
    const fireAndForget = this.broadcastFireAndForget.get(provider) ?? false;
    if (fireAndForget) {
      // Consume every settlement so a later rejection never surfaces as an
      // unhandled promise rejection; the caller doesn't wait for any of them.
      for (const delivery of deliveries) void delivery.catch(() => undefined);
      return;
    }
    const results = await Promise.allSettled(deliveries);
    const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failures.length > 0) {
      throw new AggregateError(
        failures.map((f) => f.reason),
        `broadcast publish to ${channel.namespace}/${channel.key} failed for ` +
          `${failures.length} of ${results.length} subscriber(s)`,
      );
    }
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.factory.getGrain(def, key);
  }

  /**
   * Build a reference to an explicit `GrainId` under an arbitrary interface
   * (typically a `GrainExtension`), bypassing key-type resolution from `def`.
   * Used to reach a target grain's auto-installed extension (e.g.
   * `ICancellationSourcesExtension`) whose grain id is only known as a
   * `GrainId` — the dispatcher routes the call to wherever that grain
   * actually lives, same as an ordinary `getGrain` reference.
   */
  getExtensionReference<T>(def: GrainInterface<T>, grainId: GrainId): T {
    return this.factory.getReference(def, grainId);
  }

  isActive(id: GrainId): boolean {
    return this.catalog.isActive(id);
  }

  /** The gateway `clientId` is reachable through, per this silo's gossiped client directory. */
  clientGatewayFor(clientId: GrainId): SiloAddress | undefined {
    return this.clientDirectory.lookup(clientId, this.options.local);
  }

  /** This silo's grain manifest: the interface versions it implements. */
  manifest(): SiloManifest {
    return { silo: this.options.local, entries: this.localManifestEntries() };
  }

  /** The version this silo implements for an interface, or `undefined` if none. */
  localImplementedVersion(interfaceId: number): number | undefined {
    return this.localVersions.get(interfaceId)?.version;
  }

  /** The hash ranges this silo owns on the current ring (drives reminder ownership). */
  ownedHashRanges(): Array<[number, number]> {
    return this.ring.rangesFor(this.options.local);
  }

  /**
   * Deliver a reminder tick to the grain's single activation. Routed through the
   * dispatcher (directory → placement) rather than activated locally, so the silo
   * that owns the reminder does not spin up a second activation when the grain
   * lives elsewhere; the tick reactivates an idle grain wherever it is placed.
   */
  async deliverReminder(grainId: GrainId, name: string, status: TickStatus): Promise<void> {
    await this.dispatcher.invoke({
      target: grainId,
      interfaceId: RemindableInterface.id,
      method: "receiveReminder",
      args: [name, status],
      options: {},
      reentrancyId: newChainId(),
    });
  }

  /**
   * Run one attempt of a durable job on the target grain's single activation,
   * routed through the dispatcher (directory → placement) as a `DurableJobConsumer`
   * system call — so the job runs as a turn wherever the grain is placed,
   * reactivating it if idle, exactly like reminder delivery. Returns
   * the handler's run result for the shard executor to act on.
   */
  async deliverDurableJob(job: JobRunContext): Promise<DurableJobRunResult> {
    return (await this.dispatcher.invoke({
      target: job.target,
      interfaceId: DurableJobConsumerInterface.id,
      method: "runJob",
      args: [job],
      options: {},
      reentrancyId: newChainId(),
    })) as DurableJobRunResult;
  }

  /**
   * Deliver a stream event to a subscriber grain's single activation, routed
   * through the dispatcher (directory → placement) as a `StreamConsumer` system
   * call — so a pulling agent on the queue's owner reaches the consumer wherever
   * it lives, reactivating it if idle, exactly like reminder delivery.
   */
  async deliverStreamEvent(
    grainId: GrainId,
    streamKey: string,
    event: unknown,
    token: number,
  ): Promise<void> {
    await this.dispatcher.invoke({
      target: grainId,
      interfaceId: StreamConsumerInterface.id,
      method: "deliverStreamEvent",
      args: [streamKey, event, token],
      options: {},
      reentrancyId: newChainId(),
    });
  }

  /**
   * Confirm an administratively-created subscription (`StreamSubscriptionManager.addSubscription`)
   * with the target grain's activation, routed through the dispatcher exactly
   * like `deliverStreamEvent` — resolving (and caching) its
   * `STREAM_SUBSCRIPTION_OBSERVER` without delivering an event. `false` means
   * the grain declared an observer and explicitly declined; the caller then
   * removes the subscription (Orleans: `OnSubscribed` calls `handle.UnsubscribeAsync()`
   * without ever resuming).
   */
  async confirmStreamSubscription(grainId: GrainId, streamKey: string): Promise<boolean> {
    return (await this.dispatcher.invoke({
      target: grainId,
      interfaceId: StreamConsumerInterface.id,
      method: "confirmStreamSubscription",
      args: [streamKey],
      options: {},
      reentrancyId: newChainId(),
    })) as boolean;
  }

  /**
   * Ask the elected TM whether a transaction committed, routed to its grain over
   * the dispatcher. Used by a recovering transactional resource to resolve an
   * in-doubt pending record left by a mid-commit failure.
   */
  async resolveTransactionStatus(manager: ParticipantId, transactionId: string): Promise<boolean> {
    return (await this.dispatcher.invoke({
      target: manager.grainId,
      interfaceId: TransactionResourceInterface.id,
      method: "status",
      args: [manager.stateName, transactionId],
      options: {},
      reentrancyId: newChainId(),
    })) as boolean;
  }

  /** Number of live grain activations hosted on this silo (for runtime metrics). */
  activationCount(): number {
    return this.catalog.count();
  }

  /** Cumulative location-cache hits/misses (for the directory hit-rate metric). */
  directoryCacheStats(): { hits: number; misses: number } {
    const { hits, misses } = this.cache.stats;
    return { hits, misses };
  }

  /**
   * This silo's `IGrainDirectory` (Orleans
   * `GrainDirectoryResolver.DefaultGrainDirectory`, reached in upstream tests via
   * `SiloHost.Services.GetRequiredService<GrainDirectoryResolver>()`): the pluggable
   * CAS register/lookup/unregister surface, exposed for direct testing rather than
   * only reachable indirectly through grain placement.
   */
  grainDirectory(): GrainDirectory {
    return this.directory;
  }

  /** Handed-off directory entries still retained for a successor to pull (introspection/metrics/tests). */
  pendingHandoffCount(): number {
    return this.handoffSnapshot.size;
  }

  async start(): Promise<void> {
    this.listener = await this.options.transport.listen(
      this.options.local,
      (message) => this.onMessage(message),
      (preamble, connection) => this.onClientAccept(preamble, connection),
    );
    this.collector.start();
    // Join recovery: when joining an already-running cluster, pull the live entries
    // for the ranges we now own from the incumbents instead of letting their grains
    // lazily reactivate. Only past the initial formation (version 1): at cold start
    // the peers in the view may still be coming up, there is nothing to recover yet,
    // and connecting to a not-yet-listening peer would just churn the connection pool.
    const others = this.otherActiveSilos();
    if (others.length > 0 && this.isLocalActive() && this.appliedVersion > 1) {
      this.beginRecovery(others, this.appliedVersion);
    }
  }

  async stop(): Promise<void> {
    this.collector.stop();
    // Deactivate before tearing down transport: onDeactivate hooks may make cross-silo
    // calls (e.g. notifying a watcher grain on another silo), which need the listener and
    // outbound connections still up. Only close them once every activation has drained.
    await this.catalog.deactivateAll({ code: "shutting-down", description: "node stopping" });
    await this.listener?.close();
    await this.connections.closeAll();
  }

  /**
   * Reconcile the directory with a membership view change (versioned, lossless).
   * Drop cache/connections for departed silos; in one partition pass drop entries
   * whose host silo has left (the grain is gone) and set aside entries whose range
   * the new ring assigns to another live silo (retained for that successor to pull).
   * If this silo has just joined the active set, recover the ranges it now owns
   * from the incumbents so their grains are not needlessly reactivated.
   */
  updateView(): void {
    const snapshot = this.options.membership.current();
    const local = this.options.local;
    const oldRing = this.ring;
    const newRing = this.buildRing();
    const live = new Set(activeSilos(snapshot).map((s) => s.ringKey));

    for (const member of oldRing.silos()) {
      if (!live.has(member.ringKey)) {
        this.cache.invalidateSilo(member);
        void this.connections.drop(member);
        this.clientDirectory.unregisterSilo(member);
      }
    }
    // Peer manifests may have shifted with the view (a silo upgraded/left);
    // drop them all and re-fetch lazily on the next version-aware placement.
    this.manifestCache.clear();
    this.manifestInflight.clear();

    const handedOff = this.partition.drain((entry) => {
      if (!live.has(entry.silo.ringKey)) return "drop"; // host gone — grain reactivates
      return newRing.ownerOf(entry.grainId).equals(local) ? "keep" : "handoff";
    });
    // Merge, don't replace: an entry already retained from a PRIOR handoff whose
    // successor hasn't pulled it yet must survive an unrelated view change (some
    // other silo joining/leaving) — only an ACK or expiry removes it.
    const producedAt = this.time.now();
    for (const entry of handedOff) {
      this.handoffSnapshot.set(entry.grainId.toString(), { entry, producedAt });
    }
    this.pruneHandoffSnapshot(live);

    const wasActive = oldRing.silos().some((s) => s.equals(local));
    this.ring = newRing;
    this.appliedVersion = snapshot.version;
    this.resolveViewWaiters();

    if (!wasActive && live.has(local.ringKey)) {
      this.beginRecovery(this.otherActiveSilos(), snapshot.version);
    }
  }

  private buildRing(): ConsistentHashRing {
    return new ConsistentHashRing(activeSilos(this.options.membership.current()));
  }

  private ownsNow(grainId: GrainId): boolean {
    return this.ring.ownerOf(grainId).equals(this.options.local);
  }

  private isLocalActive(): boolean {
    return activeSilos(this.options.membership.current()).some((s) => s.equals(this.options.local));
  }

  private otherActiveSilos(): SiloAddress[] {
    return activeSilos(this.options.membership.current()).filter(
      (s) => !s.equals(this.options.local),
    );
  }

  /** Resolve `appliedVersion` reaching `version`; self-advance if our view already shows it. */
  private async awaitView(version: number): Promise<void> {
    if (this.appliedVersion >= version) return;
    if (this.options.membership.current().version >= version) {
      this.updateView();
      if (this.appliedVersion >= version) return;
    }
    await new Promise<void>((resolve, reject) => {
      const onResolve = (): void => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        this.viewWaiters = this.viewWaiters.filter((w) => w.resolve !== onResolve);
        reject(new RejectionError(`membership view ${version} not reached`, "staleView"));
      }, this.callTimeoutMs);
      this.viewWaiters.push({ version, resolve: onResolve });
    });
  }

  private resolveViewWaiters(): void {
    const remaining: typeof this.viewWaiters = [];
    for (const w of this.viewWaiters) {
      if (this.appliedVersion >= w.version) w.resolve();
      else remaining.push(w);
    }
    this.viewWaiters = remaining;
  }

  /** Owned reads wait on an in-flight join recovery so they never see a transient miss. */
  private async awaitRecovered(_grainId: GrainId): Promise<void> {
    if (this.recovery !== undefined) await this.recovery;
  }

  /**
   * Pull the ranges we now own from the given previous owners and merge them in.
   * Each source is retried (bounded, backed off) independently, so one slow or
   * flaky peer doesn't stall recovery from the others. A source that exhausts
   * its retry budget degrades to lazy rebuild for that source's ranges only.
   * Concurrent registers for the recovering range are gated on
   * `LocalDirectoryPartition`'s own `recoveryMembershipVersion` for the
   * duration, in addition to `awaitRecovered` gating reads on `this.recovery`.
   */
  private beginRecovery(sources: readonly SiloAddress[], version: number): void {
    if (sources.length === 0) return;
    const newRing = this.ring;
    this.partition.beginRecovery(version);
    const done = Promise.all(
      sources.map(async (owner) => {
        try {
          const entries = await executeWithRetries<GrainAddress[] | undefined>(
            async () =>
              (await this.sendDirectory(owner, { kind: "recover", version })) as
                | GrainAddress[]
                | undefined,
            {
              maxRetries: this.recoveryMaxAttempts,
              shouldRetry: (_attempt, outcome) => outcome.kind === "error",
              backoff: fixedBackoff(this.recoveryBackoffMs),
              timeProvider: this.time,
            },
          );
          if (entries !== undefined && entries.length > 0) {
            this.partition.acceptHandoff(entries, (e) =>
              newRing.ownerOf(e.grainId).equals(this.options.local),
            );
            // ACK-delete: tell the source it can drop exactly what it just served.
            // Best-effort — a lost ACK just means the entries expire there instead.
            void this.sendDirectory(owner, {
              kind: "recoverAck",
              grainIds: entries.map((e) => e.grainId),
              version,
            }).catch(() => undefined);
          }
        } catch {
          // retries exhausted: this source's ranges degrade to lazy rebuild
        }
      }),
    ).then(() => undefined);
    this.recovery = done;
    void done.finally(() => {
      if (this.recovery === done) this.recovery = undefined;
      this.partition.endRecovery(version);
    });
  }

  /** Serve a successor's recovery pull: the entries we handed off whose host is still live. */
  private serveRecover(): GrainAddress[] {
    const live = new Set(activeSilos(this.options.membership.current()).map((s) => s.ringKey));
    this.pruneHandoffSnapshot(live);
    return [...this.handoffSnapshot.values()].map((v) => v.entry);
  }

  /** A puller's ACK for a completed pull: drop exactly the entries it confirmed applying. */
  private ackServedRecovery(grainIds: readonly GrainId[]): void {
    for (const grainId of grainIds) {
      this.handoffSnapshot.delete(grainId.toString());
    }
  }

  /**
   * Drop handed-off entries that can no longer be usefully served: the host
   * they point at fell out of the live view (the grain reactivates elsewhere
   * regardless), or they've sat unpulled past `recoveryRetentionMs` — a
   * successor that will never pull them (crashed, or never existed) must not
   * pin this memory forever.
   */
  private pruneHandoffSnapshot(live: ReadonlySet<string>): void {
    const now = this.time.now();
    for (const [key, { entry, producedAt }] of this.handoffSnapshot) {
      if (!live.has(entry.silo.ringKey) || now - producedAt > this.recoveryRetentionMs) {
        this.handoffSnapshot.delete(key);
      }
    }
  }

  private placementFor(grainType: GrainType): PlacementStrategy {
    const reg = this.grainTypes.get(grainType);
    return reg ? placementStrategyFor(reg.metadata) : randomPlacement;
  }

  /** True once this silo declares a version > 1 or a versioning policy is set. */
  private versioningActive(): boolean {
    return this.versionPolicyConfigured || this.maxLocalVersion > 1;
  }

  /**
   * Version-aware placement pre-filter. Inert (returns the candidates
   * unchanged, with no manifest round-trips) unless versioning is active.
   */
  private applyVersionFilter(
    req: InvocationRequest,
    candidates: readonly SiloAddress[],
  ): Promise<readonly SiloAddress[]> {
    if (!this.versioningActive()) return Promise.resolve(candidates);
    return filterByVersion(req.interfaceId, req.interfaceVersion ?? 1, candidates, {
      director: this.director,
      selector: this.selector,
      getManifest: (silo) => this.getManifest(silo),
    });
  }

  private localManifestEntries(): InterfaceVersionEntry[] {
    return [...this.localVersions.entries()].map(([interfaceId, v]) => ({
      interfaceId,
      version: v.version,
      grainType: v.grainType,
    }));
  }

  /** A peer's manifest: served locally, else fetched once over the transport and cached. */
  private getManifest(silo: SiloAddress): Promise<SiloManifest> {
    if (silo.equals(this.options.local)) return Promise.resolve(this.manifest());
    const key = silo.toString();
    const cached = this.manifestCache.get(key);
    if (cached !== undefined) return Promise.resolve(cached);
    const inflight = this.manifestInflight.get(key);
    if (inflight !== undefined) return inflight;
    const pending = this.sendManifest(silo).then(
      (manifest) => {
        this.manifestCache.set(key, manifest);
        this.manifestInflight.delete(key);
        return manifest;
      },
      (err: unknown) => {
        this.manifestInflight.delete(key);
        throw err;
      },
    );
    this.manifestInflight.set(key, pending);
    return pending;
  }

  private filtersFor(grainType: GrainType): readonly PlacementFilter[] {
    const reg = this.grainTypes.get(grainType);
    return reg ? placementFiltersFor(reg.metadata, this.options.placementFilterRegistry) : [];
  }

  /**
   * Placement context for the current view: activation counts, advertised silo
   * metadata, and resource stats. The local silo's metadata and load are known
   * directly; a peer's come from its membership entry (`resourceStats` is left
   * undefined unless membership carries it — there is no cross-silo load gossip yet).
   */
  private placementContext(): Omit<PlacementContext, "localSilo"> {
    const snapshot = this.options.membership.current();
    const byKey = new Map(snapshot.silos.map((m) => [m.address.ringKey, m]));
    const localMeta = this.options.metadata;
    const isLocal = (silo: SiloAddress) => silo.equals(this.options.local);
    return {
      activationCount: (silo) => (isLocal(silo) ? this.catalog.count() : 0),
      siloMetadata: (silo) => (isLocal(silo) ? localMeta : byKey.get(silo.ringKey)?.metadata),
      resourceStats: (silo) =>
        isLocal(silo) ? { activationCount: this.catalog.count() } : undefined,
      isOverloaded: (silo) =>
        isLocal(silo)
          ? this.overloadDetector.isOverloaded
          : (this.remoteLoadStats.get(silo.ringKey)?.isOverloaded ?? false),
      random: this.options.random ?? Math.random,
    };
  }

  private resolveGrainType(interfaceId: number): GrainType {
    const grainType = this.interfaceToGrainType.get(interfaceId);
    if (grainType === undefined)
      throw new Error(`no grain registered for interface ${interfaceId}`);
    return grainType;
  }

  private rehydrate(id: GrainReferenceIdentity): unknown {
    const iface = getGrainInterface(id.interfaceId);
    if (iface === undefined) throw new GrainCallError(`unknown interface ${id.interfaceId}`);
    // Use the wire `grainId` as-is rather than re-resolving a type from the
    // interface: for a normal grain reference it is the same type `getGrain`
    // would produce, but for an observer reference it preserves the reserved
    // `$client` type + `+scope` key, which re-resolution would discard.
    return this.factory.getReference(iface, id.grainId);
  }

  private onDeactivated(activation: ActivationData): void {
    void this.directory
      .unregister({
        grainId: activation.id,
        silo: this.options.local,
        activationId: activation.activationId,
      })
      .catch(() => undefined); // best-effort; the silo may be shutting down
    this.cache.invalidate(activation.id);
  }

  // --- migration ---

  /**
   * Move an idle activation to another silo with its state preserved: pick the
   * target (directed or via placement), dehydrate, and ask the target to take it
   * over (which flips the directory by CAS). Returns whether the move succeeded;
   * on any failure the caller falls back to plain deactivation.
   */
  private async migrateActivation(activation: ActivationData): Promise<boolean> {
    try {
      // Mirrors `DistributedDispatcher.placeAndInvoke`: the whole target
      // decision (filtering + strategy/directed choice) runs inside a "place
      // grain" span (Orleans `PlaceGrainAsync`, invoked from the migration
      // path as well as fresh activation), with one `FilterPlacementCandidates`
      // child span per stacked filter.
      return await withPlaceGrainSpan(async () => {
        const context: PlacementContext = {
          localSilo: this.options.local,
          activationCount: (silo) => (silo.equals(this.options.local) ? this.catalog.count() : 0),
          random: this.options.random ?? Math.random,
        };
        let candidates: readonly SiloAddress[] = activeSilos(this.options.membership.current());
        for (const filter of this.filtersFor(activation.id.type)) {
          candidates = withFilterPlacementCandidatesSpan(
            { filterType: filter.constructor.name },
            () => filter.filter(activation.id.type, candidates, context),
          );
        }
        // An explicit `migrationTarget` (set via `migrateOnIdle(target)`) wins;
        // otherwise fall back to the caller's ambient placement hint captured
        // at `requestMigration` time, resolved against the live candidates now.
        const directed =
          activation.migrationTarget ??
          resolvePlacementHintValue(activation.migrationHint, candidates);
        const target = chooseMigrationTarget(
          activation.id.type,
          directed,
          candidates,
          this.placementFor(activation.id.type),
          context,
        );
        if (target === undefined) return false;
        return this.migrateActivationTo(activation, target);
      });
    } catch {
      return false;
    }
  }

  /**
   * Migrate one activation to an explicit `target` silo (bypassing placement):
   * dehydrate its state on a turn, hand it to the target, and on acceptance drop
   * the stale cache entry. Used both by idle migration (target chosen by
   * placement) and by the rebalancer (target chosen to even out load).
   */
  private async migrateActivationTo(
    activation: ActivationData,
    target: SiloAddress,
  ): Promise<boolean> {
    try {
      const bag = await activation.dehydrate(target);
      const sourceAddr: GrainAddress = {
        grainId: activation.id,
        silo: this.options.local,
        activationId: activation.activationId,
      };
      const accepted = await this.sendMigration(target, {
        grainId: activation.id,
        sourceAddr,
        bag,
      });
      // The directory now points at the new host; drop our stale cache entry.
      if (accepted) this.cache.invalidate(activation.id);
      return accepted;
    } catch {
      return false;
    }
  }

  /** Shared boilerplate for a `system:` request: connect, correlate, send, await, interpret. */
  private async sendSystemMessage(
    target: SiloAddress,
    system: NonNullable<Message["system"]>,
    targetGrain: GrainId,
    body: Uint8Array,
  ): Promise<unknown> {
    const conn = await this.connections.get(target);
    const correlationId = nextCorrelationId();
    const message: Message = {
      correlationId,
      direction: "request",
      system,
      targetGrain,
      sendingSilo: this.options.local,
      interfaceId: 0,
      method: "",
      body,
    };
    const pending = this.correlation.register(correlationId, this.callTimeoutMs);
    conn.send(message);
    return this.interpretResponse(await pending);
  }

  private async sendMigration(target: SiloAddress, payload: MigrationPayload): Promise<boolean> {
    return (await this.sendSystemMessage(
      target,
      "migration",
      payload.grainId,
      this.serializer.serialize(payload),
    )) as boolean;
  }

  private async handleMigration(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    try {
      const payload = this.serializer.deserialize<MigrationPayload>(message.body);
      const accepted = await this.acceptMigration(payload);
      await this.reply(
        replyTo,
        responseTo(message, "success", this.serializer.serialize(accepted), this.options.local),
      );
    } catch (err) {
      const body = this.serializer.serialize({
        message: err instanceof Error ? err.message : String(err),
      });
      await this.reply(replyTo, responseTo(message, "error", body, this.options.local));
    }
  }

  /** Take over a migrating activation here, rehydrating its state and claiming the directory entry. */
  private async acceptMigration(payload: MigrationPayload): Promise<boolean> {
    const activation = await this.catalog.activateMigrated(
      payload.grainId,
      newActivationId(),
      payload.bag,
      payload.sourceAddr,
    );
    const newAddr: GrainAddress = {
      grainId: payload.grainId,
      silo: this.options.local,
      activationId: activation.activationId,
    };
    const winner = await this.directory.register(newAddr, payload.sourceAddr);
    this.cache.put(winner);
    if (!grainAddressEquals(winner, newAddr)) {
      // Another silo moved or activated the grain first: discard our activation.
      await activation.deactivate({ code: "runtime-requested", description: "migration lost CAS" });
      return false;
    }
    return true;
  }

  // --- client directory ---

  /**
   * Fires when the transport accepts an inbound connection. A plain silo peer
   * carries no `clientId` in its preamble — ignore it (silo-to-silo traffic
   * uses `onMessage` directly, not this hook). A client connection is held so
   * this silo could reach it (Orleans' gateway learning a connected client),
   * recorded in the local `ClientDirectory`, and gossiped to every other
   * active silo so the whole cluster learns which gateway the client is on.
   */
  private onClientAccept(preamble: ConnectionPreamble, connection: Connection): void {
    const clientId = preamble.clientId;
    if (clientId === undefined) return;
    this.clientConnections.set(clientId.toString(), connection);
    this.clientDirectory.register(clientId, this.options.local);
    this.broadcastClientGossip({ op: "register", clientId, gateway: this.options.local });
  }

  /** Fire-and-forget a `system: "client"` gossip message to every other active silo. */
  private broadcastClientGossip(gossip: ClientGossip): void {
    const body = this.serializer.serialize(gossip);
    for (const peer of this.otherActiveSilos()) {
      this.connections
        .get(peer)
        .then((conn) => {
          conn.send({
            correlationId: nextCorrelationId(),
            direction: "oneWay",
            system: "client",
            targetGrain: gossip.clientId,
            sendingSilo: this.options.local,
            interfaceId: 0,
            method: "",
            body,
          });
        })
        .catch(() => undefined); // best-effort: an unreachable peer just misses this update
    }
  }

  /** Apply an inbound client-directory gossip update (oneWay; never replies). */
  private handleClientGossip(message: Message): void {
    const gossip = this.serializer.deserialize<ClientGossip>(message.body);
    if (gossip.op === "register") this.clientDirectory.register(gossip.clientId, gossip.gateway);
    else this.clientDirectory.unregister(gossip.clientId, gossip.gateway);
  }

  // --- client routing (dispatcher divert for observer calls) ---

  /**
   * Route a call targeting a client-hosted observer: resolve the client's
   * gateway from the (gossiped) client directory, then either forward it
   * directly to the held connection if the client is connected here, or hop
   * to the gateway silo (whose `receiveRequest` proxies it on, and relays the
   * client's reply back to us).
   */
  private async routeToClient(req: InvocationRequest): Promise<unknown> {
    const client = clientIdOf(req.target);
    const gateway = this.clientDirectory.lookup(client, this.options.local);
    if (gateway === undefined) {
      throw new RejectionError(`no gateway for client ${client.toString()}`, "unknownTarget");
    }
    if (gateway.equals(this.options.local)) return this.deliverToProxy(req);
    return this.sendRemote(gateway, req);
  }

  /** The client is connected here: forward the call over the held connection and await its reply. */
  private async deliverToProxy(req: InvocationRequest): Promise<unknown> {
    const conn = this.clientConnections.get(clientIdOf(req.target).toString());
    if (conn === undefined) {
      throw new RejectionError(
        `client not connected here: ${clientIdOf(req.target).toString()}`,
        "unknownTarget",
      );
    }
    const correlationId = nextCorrelationId();
    const message: Message = {
      correlationId,
      direction: req.options.oneWay ? "oneWay" : "request",
      targetGrain: req.target,
      sendingSilo: this.options.local,
      sendingGrain: req.sender,
      ...(req.callingGrain !== undefined ? { callingGrain: req.callingGrain } : {}),
      interfaceId: req.interfaceId,
      ...(req.interfaceVersion !== undefined ? { interfaceVersion: req.interfaceVersion } : {}),
      method: req.method,
      requestContext: {
        reentrancyId: req.reentrancyId,
        ...(req.headers !== undefined ? { headers: req.headers } : {}),
      },
      body: this.serializer.serialize(req.args),
    };
    if (req.options.oneWay) {
      conn.send(message);
      return undefined;
    }
    const pending = this.correlation.register(correlationId, this.callTimeoutMs);
    conn.send(message);
    return this.interpretResponse(await pending);
  }

  /**
   * A request arrived here targeting a client-hosted observer (this silo is
   * the gateway): proxy it to the held client connection and relay the
   * client's reply to the original caller under the original correlation id.
   */
  private async proxyRequestToClient(message: Message): Promise<void> {
    const conn = this.clientConnections.get(clientIdOf(message.targetGrain).toString());
    const replyTo = message.sendingSilo;
    if (conn === undefined) {
      if (message.direction === "oneWay" || replyTo === undefined) return;
      await this.reply(
        replyTo,
        responseTo(
          message,
          "rejection",
          this.serializer.serialize({
            message: `client not connected: ${clientIdOf(message.targetGrain).toString()}`,
            kind: "unknownTarget",
          }),
          this.options.local,
        ),
      );
      return;
    }
    const forward: Message = {
      ...message,
      correlationId: nextCorrelationId(),
      sendingSilo: this.options.local,
    };
    if (message.direction === "oneWay") {
      conn.send(forward);
      return;
    }
    try {
      const pending = this.correlation.register(forward.correlationId, this.callTimeoutMs);
      conn.send(forward);
      const clientResponse = await pending;
      if (replyTo === undefined) return;
      const relayed = responseTo(
        message,
        clientResponse.responseKind ?? "error",
        clientResponse.body,
        this.options.local,
      );
      await this.reply(replyTo, relayed);
    } catch (err) {
      if (replyTo === undefined) return;
      const { kind, body } = this.serializeError(err);
      await this.reply(replyTo, responseTo(message, kind, body, this.options.local));
    }
  }

  // --- transport ---

  private async sendRemote(silo: SiloAddress, req: InvocationRequest): Promise<unknown> {
    const conn = await this.connections.get(silo);
    const correlationId = nextCorrelationId();
    const message: Message = {
      correlationId,
      direction: req.options.oneWay ? "oneWay" : "request",
      targetGrain: req.target,
      sendingSilo: this.options.local,
      sendingGrain: req.sender,
      ...(req.callingGrain !== undefined ? { callingGrain: req.callingGrain } : {}),
      interfaceId: req.interfaceId,
      ...(req.interfaceVersion !== undefined ? { interfaceVersion: req.interfaceVersion } : {}),
      method: req.method,
      requestContext: {
        reentrancyId: req.reentrancyId,
        ...(req.transaction !== undefined
          ? {
              transaction: {
                id: req.transaction.id,
                timeStamp: req.transaction.timeStamp,
                readOnly: req.transaction.readOnly,
              },
            }
          : {}),
        ...(req.headers !== undefined ? { headers: req.headers } : {}),
      },
      body: this.serializer.serialize(req.args),
    };
    if (req.options.oneWay) {
      conn.send(message);
      return undefined;
    }
    const pending = this.correlation.register(correlationId, this.callTimeoutMs);
    conn.send(message);
    const response = await pending;
    // Merge the participants the callee (and its sub-calls) enlisted back into
    // the ambient transaction, so the root agent commits/aborts them too. Done
    // even on an error reply, so an aborting transaction releases remote locks.
    this.mergeParticipants(req.transaction, response);
    return this.interpretResponse(response);
  }

  /** Fold a reply's enlisted participants into the caller's transaction. */
  private mergeParticipants(tx: TransactionInfo | undefined, response: Message): void {
    if (tx === undefined || response.transactionParticipants === undefined) return;
    for (const p of response.transactionParticipants) {
      const key = participantKey(p.id);
      const existing = tx.participants.get(key);
      if (existing === undefined) {
        // No live object here: the agent reaches it over the dispatcher.
        tx.participants.set(key, { id: p.id, access: p.access });
      } else {
        existing.access.reads += p.access.reads;
        existing.access.writes += p.access.writes;
      }
    }
  }

  /**
   * Whether `message` arrived directly from an external client rather than a
   * fellow cluster member: a plain (non-system) request/oneWay whose
   * `sendingSilo` is not in the active membership view. Messages this silo
   * proxies for another gateway, or routine inter-silo dispatcher/directory
   * traffic, always carry a `sendingSilo` that IS an active member, so this is
   * a safe stand-in for "this silo is acting as this call's gateway" without a
   * dedicated per-connection flag.
   */
  private isClientOriginatedRequest(message: Message): boolean {
    if (message.system !== undefined) return false;
    const from = message.sendingSilo;
    if (from === undefined) return false;
    return !activeSilos(this.options.membership.current()).some((s) => s.equals(from));
  }

  private interpretResponse(response: Message): unknown {
    if (response.responseKind === "success") return this.serializer.deserialize(response.body);
    if (response.responseKind === "rejection") {
      const payload = this.serializer.deserialize<RejectionPayload>(response.body);
      if (payload.kind === "overloaded") throw new GatewayTooBusyException(payload.message);
      throw new RejectionError(payload.message, payload.kind);
    }
    const payload = this.serializer.deserialize<{ message: string }>(response.body);
    throw new GrainCallError(payload.message);
  }

  private onMessage(message: Message): void {
    if (message.direction === "response") {
      this.correlation.complete(message);
      return;
    }
    if (message.system === "directory") {
      void this.handleDirectoryRequest(message);
      return;
    }
    if (message.system === "migration") {
      void this.handleMigration(message);
      return;
    }
    if (message.system === "manifest") {
      void this.handleManifestRequest(message);
      return;
    }
    if (message.system === "load") {
      void this.handleLoadRequest(message);
      return;
    }
    if (message.system === "loadstats") {
      void this.handleLoadStatsRequest(message);
      return;
    }
    if (message.system === "stats") {
      void this.handleStatsRequest(message);
      return;
    }
    if (message.system === "detailedstats") {
      void this.handleDetailedStatsRequest(message);
      return;
    }
    if (message.system === "actcount") {
      void this.handleActivationCountRequest(message);
      return;
    }
    if (message.system === "forcecollect") {
      void this.handleForceCollectRequest(message);
      return;
    }
    if (message.system === "siloping") {
      void this.handleSiloPingRequest(message);
      return;
    }
    if (message.system === "provctl") {
      void this.handleProviderControlRequest(message);
      return;
    }
    if (message.system === "rebalance") {
      void this.handleRebalanceRequest(message);
      return;
    }
    if (message.system === "repartition") {
      void this.handleRepartitionRequest(message);
      return;
    }
    if (message.system === "client") {
      this.handleClientGossip(message);
      return;
    }
    void this.receiveRequest(message);
  }

  private async sendDirectory(owner: SiloAddress, op: DirectoryOp): Promise<unknown> {
    return this.sendSystemMessage(
      owner,
      "directory",
      directoryOpGrainId(op),
      this.serializer.serialize(op),
    );
  }

  private async handleDirectoryRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    try {
      const op = this.serializer.deserialize<DirectoryOp>(message.body);
      const result = await this.applyDirectoryOp(op);
      await this.reply(
        replyTo,
        responseTo(message, "success", this.serializer.serialize(result), this.options.local),
      );
    } catch (err) {
      const { kind, body } = this.serializeError(err);
      await this.reply(replyTo, responseTo(message, kind, body, this.options.local));
    }
  }

  /**
   * Map a thrown error to the `(kind, body)` of an error/rejection response.
   * An `AggregateError` (non-fire-and-forget `publishToBroadcastChannel`'s
   * aggregated subscriber failures) carries its inner error messages
   * separately so the receiving end reconstructs a real `AggregateError`
   * instead of collapsing to a generic error with no `.errors` — the wire has
   * no general exception-type fidelity, so this is a narrow, deliberate
   * exception to preserve what `MultipleSubscribersOneBadActorChannelTest`-style
   * tests assert on (`Assert.Single(ex.InnerExceptions)`).
   */
  private serializeError(err: unknown): { kind: ResponseKind; body: Uint8Array } {
    if (err instanceof RejectionError) {
      return {
        kind: "rejection",
        body: this.serializer.serialize({ message: err.message, kind: err.kind }),
      };
    }
    if (err instanceof AggregateError) {
      return {
        kind: "error",
        body: this.serializer.serialize({
          message: err.message,
          aggregateMessages: err.errors.map((e) => (e instanceof Error ? e.message : String(e))),
        }),
      };
    }
    return {
      kind: "error",
      body: this.serializer.serialize({
        message: err instanceof Error ? err.message : String(err),
      }),
    };
  }

  /** Fetch a peer's manifest as a `system: "manifest"` request (mirrors directory RPC). */
  private async sendManifest(owner: SiloAddress): Promise<SiloManifest> {
    const entries = (await this.sendSystemMessage(
      owner,
      "manifest",
      new GrainId("manifest", owner.ringKey),
      this.serializer.serialize(null),
    )) as InterfaceVersionEntry[];
    return { silo: owner, entries };
  }

  private async handleManifestRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    await this.reply(
      replyTo,
      responseTo(
        message,
        "success",
        this.serializer.serialize(this.localManifestEntries()),
        this.options.local,
      ),
    );
  }

  // ── Load-aware placement (Orleans `DeploymentLoadPublisher`) ────────────────

  /**
   * Push this silo's current load snapshot (activation count + overloaded flag)
   * to every other active silo, as `system: "loadstats"` requests, and wait for
   * all of them to land — the test-only stand-in for Orleans'
   * `DeploymentLoadPublisher` periodic gossip, forced immediately (mirrors
   * `PropagateStatisticsToCluster`'s `ForceRuntimeStatisticsCollection` call)
   * so `IActivationCountBasedPlacementTestGrain.LatchOverloaded`/`LatchCpuUsage`
   * are immediately visible to placement decisions on every other silo.
   * Best-effort: an unreachable peer is skipped rather than failing the call.
   */
  async publishLoadStats(): Promise<void> {
    const peers = activeSilos(this.options.membership.current()).filter(
      (s) => !s.equals(this.options.local),
    );
    const stats = {
      activationCount: this.catalog.count(),
      isOverloaded: this.overloadDetector.isOverloaded,
    };
    await Promise.all(
      peers.map((peer) => this.sendLoadStatsPush(peer, stats).catch(() => undefined)),
    );
  }

  private async sendLoadStatsPush(
    silo: SiloAddress,
    stats: { activationCount: number; isOverloaded: boolean },
  ): Promise<void> {
    await this.sendSystemMessage(
      silo,
      "loadstats",
      new GrainId("loadstats", silo.ringKey),
      this.serializer.serialize(stats),
    );
  }

  private async handleLoadStatsRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    const stats = this.serializer.deserialize<{ activationCount: number; isOverloaded: boolean }>(
      message.body,
    );
    this.remoteLoadStats.set(replyTo.ringKey, stats);
    await this.reply(
      replyTo,
      responseTo(message, "success", this.serializer.serialize(null), this.options.local),
    );
  }

  // ── Activation rebalancer ──────────────────────────────────────────────────

  /** Ask a peer for its current activation count as a `system: "load"` request. */
  private async sendLoadQuery(silo: SiloAddress): Promise<number> {
    return (await this.sendSystemMessage(
      silo,
      "load",
      new GrainId("load", silo.ringKey),
      this.serializer.serialize(null),
    )) as number;
  }

  private async handleLoadRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    await this.reply(
      replyTo,
      responseTo(
        message,
        "success",
        this.serializer.serialize(this.catalog.count()),
        this.options.local,
      ),
    );
  }

  /**
   * Per-silo activation counts across the active cluster, keyed by ring key — the
   * load snapshot the rebalancer's model consumes. The local count is read
   * directly; peers answer a `load` query (an unreachable peer counts as 0).
   */
  async gatherClusterLoad(): Promise<{
    counts: Map<string, number>;
    silos: Map<string, SiloAddress>;
  }> {
    const active = activeSilos(this.options.membership.current());
    const counts = new Map<string, number>();
    const silos = new Map<string, SiloAddress>();
    await Promise.all(
      active.map(async (silo) => {
        const key = silo.ringKey;
        silos.set(key, silo);
        const count = silo.equals(this.options.local)
          ? this.catalog.count()
          : await this.sendLoadQuery(silo).catch(() => 0);
        counts.set(key, count);
      }),
    );
    return { counts, silos };
  }

  /** Ask a peer for its per-grain-type activation counts as a `system: "stats"` request. */
  private async sendStatsQuery(silo: SiloAddress): Promise<[GrainType, number][]> {
    return (await this.sendSystemMessage(
      silo,
      "stats",
      new GrainId("stats", silo.ringKey),
      this.serializer.serialize(null),
    )) as [GrainType, number][];
  }

  private async handleStatsRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    await this.reply(
      replyTo,
      responseTo(
        message,
        "success",
        this.serializer.serialize([...this.catalog.grainTypeCounts()]),
        this.options.local,
      ),
    );
  }

  /**
   * Per-grain-type activation counts amalgamated across the active cluster —
   * the built-in management grain's `GetSimpleGrainStatistics` (Orleans
   * parity). The local counts are read directly; each peer answers a `stats`
   * query (an unreachable peer contributes nothing, same as `gatherClusterLoad`).
   */
  async gatherClusterGrainStats(): Promise<SimpleGrainStatistic[]> {
    const active = activeSilos(this.options.membership.current());
    const results: SimpleGrainStatistic[] = [];
    await Promise.all(
      active.map(async (silo) => {
        const entries = silo.equals(this.options.local)
          ? [...this.catalog.grainTypeCounts()]
          : await this.sendStatsQuery(silo).catch(() => [] as [GrainType, number][]);
        for (const [grainType, activationCount] of entries) {
          results.push({ grainType, silo, activationCount });
        }
      }),
    );
    return results;
  }

  /** Ask a peer for its per-activation detailed stats as a `system: "detailedstats"` request. */
  private async sendDetailedStatsQuery(
    silo: SiloAddress,
    types: string[] | undefined,
  ): Promise<GrainId[]> {
    return (await this.sendSystemMessage(
      silo,
      "detailedstats",
      new GrainId("detailedstats", silo.ringKey),
      this.serializer.serialize(types ?? null),
    )) as GrainId[];
  }

  private async handleDetailedStatsRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    const types = this.serializer.deserialize<string[] | null>(message.body);
    const ids = this.localDetailedGrainIds(types ?? undefined);
    await this.reply(
      replyTo,
      responseTo(message, "success", this.serializer.serialize(ids), this.options.local),
    );
  }

  /** This silo's live activation ids, optionally restricted to `types`. */
  private localDetailedGrainIds(types: string[] | undefined): GrainId[] {
    const live = this.catalog.liveActivations();
    const filtered = types === undefined ? live : live.filter((a) => types.includes(a.id.type));
    return filtered.map((a) => a.id);
  }

  /**
   * One entry per live activation amalgamated across the active cluster,
   * optionally restricted to `types` (Orleans
   * `IManagementGrain.GetDetailedGrainStatistics`), unlike
   * `gatherClusterGrainStats` which only amalgamates a per-(type, silo)
   * count.
   */
  async gatherClusterDetailedGrainStatistics(types?: string[]): Promise<DetailedGrainStatistic[]> {
    const active = activeSilos(this.options.membership.current());
    const results: DetailedGrainStatistic[] = [];
    await Promise.all(
      active.map(async (silo) => {
        const ids = silo.equals(this.options.local)
          ? this.localDetailedGrainIds(types)
          : await this.sendDetailedStatsQuery(silo, types).catch(() => [] as GrainId[]);
        for (const grainId of ids) {
          results.push({ grainType: grainId.type, silo, grainId });
        }
      }),
    );
    return results;
  }

  /** Ask a peer for its local live activation count for one grain id, as a `system: "actcount"` request. */
  private async sendActivationCountQuery(silo: SiloAddress, id: GrainId): Promise<number> {
    return (await this.sendSystemMessage(
      silo,
      "actcount",
      id,
      this.serializer.serialize(id),
    )) as number;
  }

  private async handleActivationCountRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    const id = this.serializer.deserialize<GrainId>(message.body);
    await this.reply(
      replyTo,
      responseTo(
        message,
        "success",
        this.serializer.serialize(this.catalog.countFor(id)),
        this.options.local,
      ),
    );
  }

  /**
   * Live activation count for one grain id, amalgamated across the active
   * cluster (Orleans `IManagementGrain.GetGrainActivationCount`) — works for
   * a `[StatelessWorker]` id too, unlike directory lookup, since it sums each
   * silo's local count instead of resolving a single address.
   */
  async gatherGrainActivationCount(id: GrainId): Promise<number> {
    const active = activeSilos(this.options.membership.current());
    const counts = await Promise.all(
      active.map((silo) =>
        silo.equals(this.options.local)
          ? Promise.resolve(this.catalog.countFor(id))
          : this.sendActivationCountQuery(silo, id).catch(() => 0),
      ),
    );
    return counts.reduce((sum, n) => sum + n, 0);
  }

  /** Tell a peer to run a forced idle-activation sweep, as a `system: "forcecollect"` request. */
  private async sendForceCollect(silo: SiloAddress, ageLimitMs: number): Promise<void> {
    await this.sendSystemMessage(
      silo,
      "forcecollect",
      new GrainId("forcecollect", silo.ringKey),
      this.serializer.serialize(ageLimitMs),
    );
  }

  private async handleForceCollectRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    const ageLimitMs = this.serializer.deserialize<number>(message.body);
    await this.catalog.collectIdle(ageLimitMs);
    await this.reply(
      replyTo,
      responseTo(message, "success", this.serializer.serialize(null), this.options.local),
    );
  }

  /**
   * Force an immediate idle-activation sweep on every active silo, with
   * `ageLimitMs` in place of each activation's own configured collection age
   * (Orleans `IManagementGrain.ForceActivationCollection(TimeSpan)`).
   */
  async forceActivationCollectionCluster(ageLimitMs: number): Promise<void> {
    const active = activeSilos(this.options.membership.current());
    await Promise.all(
      active.map((silo) =>
        silo.equals(this.options.local)
          ? this.catalog.collectIdle(ageLimitMs)
          : this.sendForceCollect(silo, ageLimitMs).catch(() => undefined),
      ),
    );
  }

  /**
   * Ping a specific silo's control target (Orleans `ISiloControl.Ping`): a
   * health-check no-op that resolves once the target silo acknowledges. The
   * local silo answers in-process; a peer answers a `system: "siloping"`
   * request.
   */
  async pingSilo(target: SiloAddress, message?: string): Promise<void> {
    if (target.equals(this.options.local)) return;
    await this.sendSiloPing(target, message);
  }

  /** Tell a peer to answer a health-check ping, as a `system: "siloping"` request. */
  private async sendSiloPing(silo: SiloAddress, message?: string): Promise<void> {
    await this.sendSystemMessage(
      silo,
      "siloping",
      new GrainId("siloping", silo.ringKey),
      this.serializer.serialize(message ?? null),
    );
  }

  private async handleSiloPingRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    try {
      await this.reply(
        replyTo,
        responseTo(message, "success", this.serializer.serialize(null), this.options.local),
      );
    } catch (err) {
      const { kind, body } = this.serializeError(err);
      await this.reply(replyTo, responseTo(message, kind, body, this.options.local));
    }
  }

  /**
   * Run one `IControllable.ExecuteCommand` locally against the named
   * provider (Orleans `SendControlCommandToProvider`'s per-silo dispatch).
   * Only a stream provider that implements `Controllable` accepts this;
   * anything else (missing provider, or one that doesn't support control
   * commands) throws.
   */
  private async executeProviderCommand(
    providerName: string,
    command: number,
    arg: unknown,
  ): Promise<unknown> {
    const provider = this.streamProvider(providerName) as Partial<Controllable> | undefined;
    if (provider !== undefined && typeof provider.executeCommand === "function") {
      return provider.executeCommand(command, arg);
    }
    throw new Error(`no controllable provider named '${providerName}'`);
  }

  /** Tell a peer to run a provider control command, as a `system: "provctl"` request. */
  private async sendProviderControl(
    silo: SiloAddress,
    providerName: string,
    command: number,
    arg: unknown,
  ): Promise<unknown> {
    return this.sendSystemMessage(
      silo,
      "provctl",
      new GrainId("provctl", silo.ringKey),
      this.serializer.serialize({ providerName, command, arg }),
    );
  }

  private async handleProviderControlRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    try {
      const { providerName, command, arg } = this.serializer.deserialize<{
        providerName: string;
        command: number;
        arg: unknown;
      }>(message.body);
      const result = await this.executeProviderCommand(providerName, command, arg);
      await this.reply(
        replyTo,
        responseTo(message, "success", this.serializer.serialize(result), this.options.local),
      );
    } catch (err) {
      const { kind, body } = this.serializeError(err);
      await this.reply(replyTo, responseTo(message, kind, body, this.options.local));
    }
  }

  /**
   * Invoke an `IControllable` control command on the named provider on every
   * active silo, returning one result per silo (Orleans
   * `IManagementGrain.SendControlCommandToProvider`), in active-silo order.
   */
  async sendControlCommandToProviderCluster(
    providerName: string,
    command: number,
    arg?: unknown,
  ): Promise<unknown[]> {
    const active = activeSilos(this.options.membership.current());
    return Promise.all(
      active.map((silo) =>
        silo.equals(this.options.local)
          ? this.executeProviderCommand(providerName, command, arg)
          : this.sendProviderControl(silo, providerName, command, arg),
      ),
    );
  }

  /**
   * Migrate up to `count` of this silo's live activations to `target`, chosen at
   * random (Orleans `MigrateRandomActivations`). Each is moved with its state and
   * then deactivated locally; returns how many actually moved. The rebalancer
   * invokes this on the busier silo of a pair to even out load.
   */
  async migrateRandomActivations(target: SiloAddress, count: number): Promise<number> {
    if (count <= 0 || target.equals(this.options.local)) return 0;
    const random = this.options.random ?? Math.random;
    const candidates = this.catalog.liveActivations();
    // Fisher–Yates over a copy, deterministic under an injected RNG.
    for (let i = candidates.length - 1; i > 0; i--) {
      const j = Math.floor(random() * (i + 1));
      [candidates[i], candidates[j]] = [candidates[j]!, candidates[i]!];
    }
    let moved = 0;
    for (const activation of candidates.slice(0, count)) {
      const accepted = await this.migrateActivationTo(activation, target);
      if (accepted) {
        await activation.deactivate({
          code: "migrating",
          description: "rebalanced to another silo",
        });
        moved++;
      }
    }
    return moved;
  }

  /** Tell `silo` to migrate `count` random activations to `target` (local fast-path or `rebalance` RPC). */
  private async sendMigrateRandom(
    silo: SiloAddress,
    target: SiloAddress,
    count: number,
  ): Promise<number> {
    if (silo.equals(this.options.local)) return this.migrateRandomActivations(target, count);
    return (await this.sendSystemMessage(
      silo,
      "rebalance",
      new GrainId("rebalance", silo.ringKey),
      this.serializer.serialize({ target, count }),
    )) as number;
  }

  private async handleRebalanceRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    const { target, count } = this.serializer.deserialize<{ target: SiloAddress; count: number }>(
      message.body,
    );
    const moved = await this.migrateRandomActivations(target, count);
    await this.reply(
      replyTo,
      responseTo(message, "success", this.serializer.serialize(moved), this.options.local),
    );
  }

  /**
   * Run one rebalancing cycle: gather the cluster load, ask the model for the
   * migrations that would lower its entropy imbalance, and execute
   * them by telling each busier silo to shed activations to its paired quieter
   * silo. Returns the model's next state and imbalance plus how many activations
   * actually moved. The elected worker (slice 2b) drives this on a timer.
   */
  async runRebalanceCycle(
    state: CycleState,
    options: RebalancerOptions = DEFAULT_REBALANCER_OPTIONS,
  ): Promise<{ nextState: CycleState; imbalance: number; moved: number; stop?: StopReason }> {
    const { counts, silos } = await this.gatherClusterLoad();
    const plan = planCycle(counts, options, state);
    let moved = 0;
    for (const move of plan.moves) {
      const from = silos.get(move.from);
      const to = silos.get(move.to);
      if (from === undefined || to === undefined) continue;
      moved += await this.sendMigrateRandom(from, to, move.count);
    }
    return {
      nextState: plan.nextState,
      imbalance: plan.imbalance,
      moved,
      ...(plan.stop !== undefined ? { stop: plan.stop } : {}),
    };
  }

  // ── Activation repartitioner ────────────────────────────────────────────

  /** Whether `grainId`'s type may be moved by `kind` (Orleans `GrainMigratabilityChecker`). */
  private isMigratableBy(grainId: GrainId, kind: "rebalancer" | "repartitioner"): boolean {
    const immovable = this.grainTypes.get(grainId.type)?.metadata.options.immovable;
    return immovable !== "any" && immovable !== kind;
  }

  /** Migrate a locally-hosted grain (by id) to `target`; `false` if not hosted here. */
  private async migrateGrainToSilo(grainId: GrainId, target: SiloAddress): Promise<boolean> {
    const activation = this.catalog.get(grainId);
    if (activation === undefined) return false;
    const accepted = await this.migrateActivationTo(activation, target);
    if (accepted) {
      await activation.deactivate({
        code: "migrating",
        description: "repartitioned to another silo",
      });
    }
    return accepted;
  }

  /** Send an `AcceptExchangeRequest` to a peer's repartitioner, as a `system: "repartition"` request. */
  private async sendAcceptExchangeRequest(
    target: SiloAddress,
    request: AcceptExchangeRequest,
  ): Promise<AcceptExchangeResponse> {
    return (await this.sendSystemMessage(
      target,
      "repartition",
      new GrainId("repartition", target.ringKey),
      this.serializer.serialize(request),
    )) as AcceptExchangeResponse;
  }

  private async handleRepartitionRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    try {
      const request = this.serializer.deserialize<AcceptExchangeRequest>(message.body);
      const response: AcceptExchangeResponse =
        this.repartitioner === undefined
          ? { type: "exchangedRecently", acceptedGrainIds: [], givenGrainIds: [] }
          : await this.repartitioner.acceptExchangeRequest(request);
      await this.reply(
        replyTo,
        responseTo(message, "success", this.serializer.serialize(response), this.options.local),
      );
    } catch (err) {
      const { kind, body } = this.serializeError(err);
      await this.reply(replyTo, responseTo(message, kind, body, this.options.local));
    }
  }

  /** Manually run one exchange round on this silo's repartitioner (test/control surface). */
  async triggerRepartitionExchange(): Promise<void> {
    await this.repartitioner?.triggerExchangeRequest();
  }

  resetRepartitionCounters(): void {
    this.repartitioner?.resetCounters();
  }

  getRepartitionActivationCount(): number {
    return this.repartitioner?.getActivationCount() ?? this.catalog.count();
  }

  setRepartitionActivationCountOffset(offset: number): void {
    this.repartitioner?.setActivationCountOffset(offset);
  }

  getRepartitionCallFrequencies(): ReadonlyArray<{ edge: Edge; count: number }> {
    return this.repartitioner?.getGrainCallFrequencies() ?? [];
  }

  async flushRepartitionBuffers(): Promise<void> {
    await this.repartitioner?.flushBuffers();
  }

  private async applyDirectoryOp(
    op: DirectoryOp,
  ): Promise<GrainAddress | GrainAddress[] | undefined> {
    // Linearise against our view: catch up if the caller is ahead of us; redirect
    // the caller if it is behind and we no longer own the target (a `staleView`
    // rejection it re-resolves), so we never serve under two ring topologies.
    if (op.version > this.appliedVersion) await this.awaitView(op.version);
    if (op.kind !== "recover" && op.kind !== "recoverAck" && op.version < this.appliedVersion) {
      const grainId = op.kind === "lookup" ? op.grainId : op.addr.grainId;
      if (!this.ownsNow(grainId)) throw new RejectionError("stale directory view", "staleView");
    }
    switch (op.kind) {
      case "lookup":
        await this.awaitRecovered(op.grainId);
        return this.partition.lookup(op.grainId);
      case "register":
        await this.awaitRecovered(op.addr.grainId);
        return this.partition.register(op.addr, op.previous, op.version);
      case "unregister":
        await this.awaitRecovered(op.addr.grainId);
        this.partition.unregister(op.addr);
        return undefined;
      case "recover":
        return this.serveRecover();
      case "recoverAck":
        this.ackServedRecovery(op.grainIds);
        return undefined;
    }
  }

  private async receiveRequest(message: Message): Promise<void> {
    if (isClient(message.targetGrain)) return this.proxyRequestToClient(message);
    const replyTo = message.sendingSilo;
    // Record this inbound call's communication edge from the receiving side
    // (Orleans' remote-to-local edge). The sending side already recorded its
    // own local-to-remote/local-to-local view in `DistributedDispatcher` via
    // `recordEdge`; only a call that truly crossed the wire from another silo
    // is recorded here, so a locally-delivered call is never double-counted.
    if (
      this.repartitioner !== undefined &&
      message.callingGrain !== undefined &&
      message.sendingSilo !== undefined &&
      !message.sendingSilo.equals(this.options.local)
    ) {
      this.repartitioner.recordRemoteEdge(
        message.callingGrain,
        message.sendingSilo,
        message.targetGrain,
      );
    }
    // Gateway load shedding (Orleans `GatewayAcceptor`/`OverloadDetector`): a
    // request arriving directly from an external client — not relayed from
    // another cluster member — is refused outright while this silo is
    // overloaded, before placement/activation ever runs. Inter-silo traffic
    // (directory ops, migrations, ordinary grain-to-grain calls) is never
    // shed here; only this silo's own gateway path is.
    if (this.isClientOriginatedRequest(message) && this.overloadDetector.isOverloaded) {
      if (message.direction === "oneWay" || replyTo === undefined) return;
      const rejection: RejectionPayload = { message: "Gateway too busy", kind: "overloaded" };
      await this.reply(
        replyTo,
        responseTo(message, "rejection", this.serializer.serialize(rejection), this.options.local),
      );
      return;
    }
    // Reconstruct the transaction context (fresh participant set: resources on
    // this silo enlist into it, and we send those back on the reply).
    const header = message.requestContext?.transaction;
    const transaction: TransactionInfo | undefined =
      header !== undefined
        ? {
            id: header.id,
            timeStamp: header.timeStamp,
            readOnly: header.readOnly,
            participants: new Map(),
            // Orphan-call tracking (`TransactionInfo.pendingCalls`) is decided
            // at the root boundary from the root's own fork/complete
            // bookkeeping; a remote hop's reconstructed context doesn't
            // separately track it.
            pendingCalls: 0,
          }
        : undefined;
    try {
      // Extract the incoming W3C `traceparent` (if any) BEFORE placement and
      // activation run, not just around method dispatch (`tracingFilters()`'s
      // incoming filter, which wraps `ActivationData.invoke` further down the
      // call chain) — so a first call that triggers activation runs
      // placement/register/activate inside the caller's trace, giving the
      // "place grain"/"activate grain"/"register directory entry" spans
      // (`@tsva/observability/activation-tracing`) the same trace id as the
      // triggering grain call, not a disconnected root trace.
      const headers = message.requestContext?.headers;
      const extracted =
        headers !== undefined ? propagation.extract(context.active(), headers) : context.active();
      const result = await context.with(extracted, () =>
        // A client-side `BroadcastChannelProvider`'s publish call: it addresses
        // no real grain activation (`broadcastPublisherGrainId()` is a fixed
        // pseudo-target), so it's handled directly here rather than going
        // through placement/directory routing the way an ordinary grain call
        // (and even `BroadcastConsumerInterface` delivery, which DOES target a
        // real subscriber activation) does.
        message.interfaceId === BroadcastChannelPublisherInterface.id
          ? this.dispatchBroadcastPublish(message)
          : this.dispatcher.deliverLocal(this.toRequest(message, transaction)),
      );
      if (message.direction === "oneWay" || replyTo === undefined) return;
      const response = responseTo(
        message,
        "success",
        this.serializer.serialize(result),
        this.options.local,
      );
      this.attachParticipants(response, transaction);
      await this.reply(replyTo, response);
    } catch (err) {
      if (message.direction === "oneWay" || replyTo === undefined) return;
      const { kind, body } = this.serializeError(err);
      const response = responseTo(message, kind, body, this.options.local);
      this.attachParticipants(response, transaction);
      await this.reply(replyTo, response);
    }
  }

  /** Deserialize and run a client-originated broadcast-channel publish (see `receiveRequest`). */
  private async dispatchBroadcastPublish(message: Message): Promise<undefined> {
    const [providerName, channel, item] = this.serializer.deserialize<[string, ChannelId, unknown]>(
      message.body,
    );
    await this.publishToBroadcastChannel(providerName, channel, item);
    return undefined;
  }

  /** Carry the participants enlisted during this call back on the reply. */
  private attachParticipants(response: Message, transaction: TransactionInfo | undefined): void {
    if (transaction === undefined || transaction.participants.size === 0) return;
    response.transactionParticipants = [...transaction.participants.values()].map((e) => ({
      id: e.id,
      access: e.access,
    }));
  }

  private toRequest(message: Message, transaction?: TransactionInfo): InvocationRequest {
    // Method dispatch is by name; the interface (if registered) only supplies the
    // per-method invocation options the receiving scheduler needs.
    const iface = getGrainInterface(message.interfaceId);
    return {
      target: message.targetGrain,
      interfaceId: message.interfaceId,
      ...(message.interfaceVersion !== undefined
        ? { interfaceVersion: message.interfaceVersion }
        : {}),
      method: message.method,
      args: this.serializer.deserialize<unknown[]>(message.body),
      options: iface?.options[message.method] ?? {},
      reentrancyId: message.requestContext?.reentrancyId ?? newChainId(),
      ...(message.sendingGrain !== undefined ? { sender: message.sendingGrain } : {}),
      ...(transaction !== undefined ? { transaction } : {}),
      ...(message.requestContext?.headers !== undefined
        ? { headers: message.requestContext.headers }
        : {}),
    };
  }

  private async reply(to: SiloAddress, message: Message): Promise<void> {
    try {
      const conn = await this.connections.get(to);
      conn.send(message);
    } catch {
      // The caller has gone (e.g. drained mid-call); dropping the reply is fine —
      // it will re-resolve on retry.
    }
  }
}
