import { newActivationId } from "@tsva/core/activation-id";
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
  BroadcastConsumerInterface,
  channelKey,
  type BroadcastChannelProvider,
  type ChannelId,
} from "@tsva/core/broadcast-channel";
import { StreamConsumerInterface, type StreamProvider } from "@tsva/core/stream";
import type { InvocationRequest } from "@tsva/core/request";
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
import type { Listener, Transport } from "@tsva/messaging/transport";
import { ActivationCollector } from "@tsva/runtime/activation-collector";
import { Catalog, type RegisteredGrain } from "@tsva/runtime/catalog";
import type { ActivationData } from "@tsva/runtime/activation";
import { DistributedDispatcher } from "@tsva/runtime/distributed-dispatcher";
import { BroadcastChannelProviderImpl } from "@tsva/runtime/broadcast-channel-provider";
import { GrainFactory } from "@tsva/runtime/grain-factory";
import { chooseMigrationTarget } from "@tsva/runtime/placement/choose-migration-target";
import {
  placementFiltersFor,
  placementStrategyFor,
} from "@tsva/runtime/placement/placement-director";
import type { PlacementFilter } from "@tsva/runtime/placement/placement-filter";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";
import type {
  PlacementContext,
  PlacementStrategy,
} from "@tsva/runtime/placement/placement-strategy";
import { filterByVersion } from "@tsva/runtime/placement/version-placement-filter";
import {
  DEFAULT_REBALANCER_OPTIONS,
  planCycle,
  type CycleState,
  type RebalancerOptions,
  type StopReason,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import { systemTimeProvider, type TimeProvider } from "@tsva/runtime/time-provider";
import { TransactionAgent } from "@tsva/runtime/transaction-agent";

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
  /** Resolves the stream provider a grain's `getStreamProvider` returns. */
  streamProvider?: (name?: string) => StreamProvider | undefined;
  /**
   * Names of broadcast-channel providers configured on this silo. Each
   * becomes a `getBroadcastChannelProvider(name)` whose writers fan out to the
   * channel's implicit subscribers. The first registered name is the default.
   */
  broadcastProviders?: readonly string[];
  /** Incoming grain-call filters wrapping each grain-method dispatch (silo-wide). */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
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
  | { kind: "recover"; version: number };

/** Stand-in target grain for batch ops with no single grain (fills the envelope only). */
const DIRECTORY_OP_TARGET = new GrainId("$directory", "op");

function directoryOpGrainId(op: DirectoryOp): GrainId {
  if (op.kind === "lookup") return op.grainId;
  if (op.kind === "recover") return DIRECTORY_OP_TARGET;
  return op.addr.grainId;
}

/** Carries a migrating activation's identity and dehydrated state to its new host. */
interface MigrationPayload {
  grainId: GrainId;
  sourceAddr: GrainAddress;
  bag: Record<string, unknown>;
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

  private readonly grainTypes = new Map<GrainType, RegisteredGrain>();
  private readonly interfaceToGrainType = new Map<number, GrainType>();
  /** Stream namespace → grain types implicitly subscribed to it (auto-subscribe by key). */
  private readonly implicitSubscriptions = new Map<string, GrainType[]>();
  /** Broadcast-channel namespace → grain types implicitly subscribed to it. */
  private readonly broadcastSubscriptions = new Map<string, GrainType[]>();
  /** Configured broadcast-channel provider names; the first is the default. */
  private readonly broadcastProviderNames: readonly string[];
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
  private ring: ConsistentHashRing;
  private listener: Listener | undefined;

  /** The membership view version `this.ring` was built from. */
  private appliedVersion: number;
  /** Entries this silo handed off at the last view change, retained for a successor to pull. */
  private handoffSnapshot: GrainAddress[] = [];
  /** In-flight range recovery after a join; owned reads wait on it so none miss. */
  private recovery: Promise<void> | undefined;
  /** Callers awaiting `this.appliedVersion` to reach a given version. */
  private viewWaiters: Array<{ version: number; resolve: () => void }> = [];

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
    this.broadcastProviderNames = options.broadcastProviders ?? [];
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
    this.factory = new GrainFactory((interfaceId) => this.resolveGrainType(interfaceId));
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
      ...(options.stateBinder !== undefined ? { activateState: options.stateBinder } : {}),
      ...(options.reminderRegistry !== undefined
        ? { reminderRegistry: options.reminderRegistry }
        : {}),
      ...(options.durableJobScheduler !== undefined
        ? { durableJobScheduler: options.durableJobScheduler }
        : {}),
      ...(options.streamProvider !== undefined ? { streamProvider: options.streamProvider } : {}),
      ...(this.broadcastProviderNames.length > 0
        ? { broadcastProvider: (name?: string) => this.broadcastChannelProvider(name) }
        : {}),
      ...(options.incomingCallFilters !== undefined
        ? { incomingCallFilters: options.incomingCallFilters }
        : {}),
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
    });
    this.factory.setDispatcher(this.dispatcher);
    const transactionAgent = new TransactionAgent(time);
    transactionAgent.setDispatcher(this.dispatcher);
    this.factory.setTransactionAgent(transactionAgent);
    if (options.outgoingCallFilters !== undefined) {
      this.factory.setOutgoingCallFilters(options.outgoingCallFilters);
    }
    this.collector = new ActivationCollector(
      this.catalog,
      time,
      (options.collectionIntervalSeconds ?? 60) * 1000,
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
    for (const namespace of metadata.broadcastSubscriptions) {
      const types = this.broadcastSubscriptions.get(namespace) ?? [];
      if (!types.includes(metadata.grainType)) types.push(metadata.grainType);
      this.broadcastSubscriptions.set(namespace, types);
    }
    return this;
  }

  /** Grain types implicitly subscribed to a stream namespace (drives stream fan-out). */
  implicitGrainTypes(namespace: string): readonly GrainType[] {
    return this.implicitSubscriptions.get(namespace) ?? [];
  }

  /** Grain types implicitly subscribed to a broadcast-channel namespace. */
  broadcastGrainTypes(namespace: string): readonly GrainType[] {
    return this.broadcastSubscriptions.get(namespace) ?? [];
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
   * idle subscriber, exactly like stream delivery. Deliveries are awaited so a
   * failing subscriber surfaces to the publisher; this diverges from Orleans'
   * fire-and-forget default in favour of error visibility.
   */
  async publishToBroadcastChannel(
    _provider: string,
    channel: ChannelId,
    item: unknown,
  ): Promise<void> {
    const key = channelKey(channel);
    const deliveries = this.broadcastGrainTypes(channel.namespace).map((type) =>
      this.dispatcher.invoke({
        target: new GrainId(type, channel.key),
        interfaceId: BroadcastConsumerInterface.id,
        method: "onPublished",
        args: [key, item],
        options: {},
        reentrancyId: newChainId(),
      }),
    );
    await Promise.all(deliveries);
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.factory.getGrain(def, key);
  }

  isActive(id: GrainId): boolean {
    return this.catalog.isActive(id);
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

  async start(): Promise<void> {
    this.listener = await this.options.transport.listen(this.options.local, (message) =>
      this.onMessage(message),
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
    await this.listener?.close();
    await this.connections.closeAll();
    await this.catalog.deactivateAll({ code: "shutting-down", description: "node stopping" });
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
      }
    }
    // Peer manifests may have shifted with the view (a silo upgraded/left);
    // drop them all and re-fetch lazily on the next version-aware placement.
    this.manifestCache.clear();
    this.manifestInflight.clear();

    this.handoffSnapshot = this.partition.drain((entry) => {
      if (!live.has(entry.silo.ringKey)) return "drop"; // host gone — grain reactivates
      return newRing.ownerOf(entry.grainId).equals(local) ? "keep" : "handoff";
    });

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

  /** Pull the ranges we now own from the given previous owners and merge them in. */
  private beginRecovery(sources: readonly SiloAddress[], version: number): void {
    if (sources.length === 0) return;
    const newRing = this.ring;
    const done = Promise.all(
      sources.map(async (owner) => {
        try {
          const entries = (await this.sendDirectory(owner, { kind: "recover", version })) as
            | GrainAddress[]
            | undefined;
          if (entries !== undefined) {
            this.partition.acceptHandoff(entries, (e) =>
              newRing.ownerOf(e.grainId).equals(this.options.local),
            );
          }
        } catch {
          // best-effort: a failed pull degrades to lazy rebuild for that source's ranges
        }
      }),
    ).then(() => undefined);
    this.recovery = done;
    void done.finally(() => {
      if (this.recovery === done) this.recovery = undefined;
    });
  }

  /** Serve a successor's recovery pull: the entries we handed off whose host is still live. */
  private serveRecover(): GrainAddress[] {
    const live = new Set(activeSilos(this.options.membership.current()).map((s) => s.ringKey));
    return this.handoffSnapshot.filter((e) => live.has(e.silo.ringKey));
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
    return reg ? placementFiltersFor(reg.metadata) : [];
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
    return this.factory.getGrain(iface, id.grainId.key);
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
      const candidates = activeSilos(this.options.membership.current());
      const context: PlacementContext = {
        localSilo: this.options.local,
        activationCount: (silo) => (silo.equals(this.options.local) ? this.catalog.count() : 0),
        random: this.options.random ?? Math.random,
      };
      const target = chooseMigrationTarget(
        activation.id.type,
        activation.migrationTarget,
        candidates,
        this.placementFor(activation.id.type),
        context,
      );
      if (target === undefined) return false;
      return await this.migrateActivationTo(activation, target);
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
      const bag = await activation.dehydrate();
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
    const activation = this.catalog.activateMigrated(
      payload.grainId,
      newActivationId(),
      payload.bag,
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

  private interpretResponse(response: Message): unknown {
    if (response.responseKind === "success") return this.serializer.deserialize(response.body);
    if (response.responseKind === "rejection") {
      const payload = this.serializer.deserialize<RejectionPayload>(response.body);
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
    if (message.system === "rebalance") {
      void this.handleRebalanceRequest(message);
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

  /** Map a thrown error to the `(kind, body)` of an error/rejection response. */
  private serializeError(err: unknown): { kind: ResponseKind; body: Uint8Array } {
    return err instanceof RejectionError
      ? {
          kind: "rejection",
          body: this.serializer.serialize({ message: err.message, kind: err.kind }),
        }
      : {
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

  private async applyDirectoryOp(
    op: DirectoryOp,
  ): Promise<GrainAddress | GrainAddress[] | undefined> {
    // Linearise against our view: catch up if the caller is ahead of us; redirect
    // the caller if it is behind and we no longer own the target (a `staleView`
    // rejection it re-resolves), so we never serve under two ring topologies.
    if (op.version > this.appliedVersion) await this.awaitView(op.version);
    if (op.kind !== "recover" && op.version < this.appliedVersion) {
      const grainId = op.kind === "lookup" ? op.grainId : op.addr.grainId;
      if (!this.ownsNow(grainId)) throw new RejectionError("stale directory view", "staleView");
    }
    switch (op.kind) {
      case "lookup":
        await this.awaitRecovered(op.grainId);
        return this.partition.lookup(op.grainId);
      case "register":
        await this.awaitRecovered(op.addr.grainId);
        return this.partition.register(op.addr, op.previous);
      case "unregister":
        await this.awaitRecovered(op.addr.grainId);
        this.partition.unregister(op.addr);
        return undefined;
      case "recover":
        return this.serveRecover();
    }
  }

  private async receiveRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
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
          }
        : undefined;
    try {
      const result = await this.dispatcher.deliverLocal(this.toRequest(message, transaction));
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
