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
import { nextCorrelationId, responseTo, type Message } from "@tsva/messaging/message";
import { MessagePackSerializer } from "@tsva/messaging/msgpack-serializer";
import type { Serializer } from "@tsva/messaging/serializer";
import type { Listener, Transport } from "@tsva/messaging/transport";
import { ActivationCollector } from "@tsva/runtime/activation-collector";
import { Catalog, type RegisteredGrain } from "@tsva/runtime/catalog";
import type { ActivationData } from "@tsva/runtime/activation";
import { DistributedDispatcher } from "@tsva/runtime/distributed-dispatcher";
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
  /** Resolves the stream provider a grain's `getStreamProvider` returns. */
  streamProvider?: (name?: string) => StreamProvider | undefined;
  /** Incoming grain-call filters wrapping each grain-method dispatch (silo-wide). */
  incomingCallFilters?: readonly IncomingGrainCallFilter[];
  /** Outgoing grain-call filters wrapping each outbound call at the proxy (silo-wide). */
  outgoingCallFilters?: readonly OutgoingGrainCallFilter[];
  /** Injectable RNG for deterministic placement in tests. */
  random?: () => number;
  /**
   * Grain-interface versioning policy (ADR 0014). Setting either field enables
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

/** A directory partition operation routed to the owning silo over the transport. */
type DirectoryOp =
  | { kind: "lookup"; grainId: GrainId }
  | { kind: "register"; addr: GrainAddress; previous?: GrainAddress | undefined }
  | { kind: "unregister"; addr: GrainAddress };

function directoryOpGrainId(op: DirectoryOp): GrainId {
  return op.kind === "lookup" ? op.grainId : op.addr.grainId;
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
  readonly partition = new LocalDirectoryPartition();

  private readonly grainTypes = new Map<GrainType, RegisteredGrain>();
  private readonly interfaceToGrainType = new Map<number, GrainType>();
  /** Stream namespace → grain types implicitly subscribed to it (auto-subscribe by key). */
  private readonly implicitSubscriptions = new Map<string, GrainType[]>();
  /** Interface versions this silo implements (ADR 0014): interfaceId -> hosted version. */
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

  /** Routes directory operations to the owning silo's partition over the transport. */
  private readonly transportPeer: DirectoryPeer = {
    lookup: (owner, grainId) => this.sendDirectory(owner, { kind: "lookup", grainId }),
    register: (owner, addr, previous) =>
      this.sendDirectory(owner, { kind: "register", addr, previous }) as Promise<GrainAddress>,
    unregister: async (owner, addr) => {
      await this.sendDirectory(owner, { kind: "unregister", addr });
    },
  };

  constructor(private readonly options: ClusterNodeOptions) {
    const time = options.time ?? systemTimeProvider;
    this.callTimeoutMs = options.callTimeoutMs ?? 30_000;
    this.versionPolicyConfigured =
      options.versionCompatibility !== undefined || options.versionSelector !== undefined;
    this.director = compatibilityDirector(options.versionCompatibility ?? "backwardCompatible");
    this.selector = versionSelector(options.versionSelector ?? "latest");
    this.ring = this.buildRing();
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
      ...(options.streamProvider !== undefined ? { streamProvider: options.streamProvider } : {}),
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
    return this;
  }

  /** Grain types implicitly subscribed to a stream namespace (drives stream fan-out). */
  implicitGrainTypes(namespace: string): readonly GrainType[] {
    return this.implicitSubscriptions.get(namespace) ?? [];
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.factory.getGrain(def, key);
  }

  isActive(id: GrainId): boolean {
    return this.catalog.isActive(id);
  }

  /** This silo's grain manifest: the interface versions it implements (ADR 0014). */
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
  }

  async stop(): Promise<void> {
    this.collector.stop();
    await this.listener?.close();
    await this.connections.closeAll();
    await this.catalog.deactivateAll({ code: "shutting-down", description: "node stopping" });
  }

  /** Recompute the ring and drop entries for silos that have left the view. */
  updateView(): void {
    const stillActive = new Set(
      activeSilos(this.options.membership.current()).map((s) => s.ringKey),
    );
    for (const member of this.ring.silos()) {
      if (!stillActive.has(member.ringKey)) {
        this.partition.unregisterSilo(member);
        this.cache.invalidateSilo(member);
        void this.connections.drop(member);
      }
    }
    // Peer manifests may have shifted with the view (a silo upgraded/left);
    // drop them all and re-fetch lazily on the next version-aware placement.
    this.manifestCache.clear();
    this.manifestInflight.clear();
    this.ring = this.buildRing();
  }

  private buildRing(): ConsistentHashRing {
    return new ConsistentHashRing(activeSilos(this.options.membership.current()));
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
   * Version-aware placement pre-filter (ADR 0014). Inert (returns the candidates
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

  private async sendMigration(target: SiloAddress, payload: MigrationPayload): Promise<boolean> {
    const conn = await this.connections.get(target);
    const correlationId = nextCorrelationId();
    const message: Message = {
      correlationId,
      direction: "request",
      system: "migration",
      targetGrain: payload.grainId,
      sendingSilo: this.options.local,
      interfaceId: 0,
      method: "",
      body: this.serializer.serialize(payload),
    };
    const pending = this.correlation.register(correlationId, this.callTimeoutMs);
    conn.send(message);
    return this.interpretResponse(await pending) as boolean;
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
    void this.receiveRequest(message);
  }

  private async sendDirectory(
    owner: SiloAddress,
    op: DirectoryOp,
  ): Promise<GrainAddress | undefined> {
    const conn = await this.connections.get(owner);
    const correlationId = nextCorrelationId();
    const message: Message = {
      correlationId,
      direction: "request",
      system: "directory",
      targetGrain: directoryOpGrainId(op),
      sendingSilo: this.options.local,
      interfaceId: 0,
      method: "",
      body: this.serializer.serialize(op),
    };
    const pending = this.correlation.register(correlationId, this.callTimeoutMs);
    conn.send(message);
    const result = this.interpretResponse(await pending);
    // A "not found" lookup serializes back as null; normalise to undefined.
    return result == null ? undefined : (result as GrainAddress);
  }

  private async handleDirectoryRequest(message: Message): Promise<void> {
    const replyTo = message.sendingSilo;
    if (replyTo === undefined) return;
    try {
      const op = this.serializer.deserialize<DirectoryOp>(message.body);
      const result = this.applyDirectoryOp(op);
      await this.reply(
        replyTo,
        responseTo(message, "success", this.serializer.serialize(result), this.options.local),
      );
    } catch (err) {
      const kind = err instanceof RejectionError ? "rejection" : "error";
      const body =
        err instanceof RejectionError
          ? this.serializer.serialize({ message: err.message, kind: err.kind })
          : this.serializer.serialize({
              message: err instanceof Error ? err.message : String(err),
            });
      await this.reply(replyTo, responseTo(message, kind, body, this.options.local));
    }
  }

  /** Fetch a peer's manifest as a `system: "manifest"` request (mirrors directory RPC). */
  private async sendManifest(owner: SiloAddress): Promise<SiloManifest> {
    const conn = await this.connections.get(owner);
    const correlationId = nextCorrelationId();
    const message: Message = {
      correlationId,
      direction: "request",
      system: "manifest",
      targetGrain: new GrainId("manifest", owner.ringKey),
      sendingSilo: this.options.local,
      interfaceId: 0,
      method: "",
      body: this.serializer.serialize(null),
    };
    const pending = this.correlation.register(correlationId, this.callTimeoutMs);
    conn.send(message);
    const entries = this.interpretResponse(await pending) as InterfaceVersionEntry[];
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

  private applyDirectoryOp(op: DirectoryOp): GrainAddress | undefined {
    switch (op.kind) {
      case "lookup":
        return this.partition.lookup(op.grainId);
      case "register":
        return this.partition.register(op.addr, op.previous);
      case "unregister":
        this.partition.unregister(op.addr);
        return undefined;
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
      const body =
        err instanceof RejectionError
          ? this.serializer.serialize({ message: err.message, kind: err.kind })
          : this.serializer.serialize({
              message: err instanceof Error ? err.message : String(err),
            });
      const kind = err instanceof RejectionError ? "rejection" : "error";
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
