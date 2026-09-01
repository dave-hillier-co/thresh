import type { Duration } from "@thresh/core/duration";
import { GrainCancellationTokenSource } from "@thresh/core/grain-cancellation-token";
import type { GrainRegistrationSpec } from "@thresh/core/grain-registration-spec";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import type { Registrable } from "@thresh/core/grain-registration";
import type { KeyTypeOf } from "@thresh/core/key-kinds";
import type { MembershipService, MembershipSnapshot } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import type { StreamProvider } from "@thresh/core/stream";
import type { TimeProvider } from "@thresh/core/time-provider";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ICancellationSourcesExtension } from "@thresh/runtime/cancellation-extension";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { MemoryStreamProvider } from "@thresh/streams/memory-stream-provider";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import type { LoadSheddingOptions } from "@thresh/runtime/load-shedding";
import type { PlacementStrategy } from "@thresh/runtime/placement/placement-strategy";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { createSilo, type SiloBuilder } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { createClient, type ClientNode } from "@thresh/client/client-node";
import { membershipGatewayProvider } from "@thresh/client/gateway-provider";

export type { GrainRegistrationSpec, ClientNode };

/**
 * What `TestClusterOptions.grains` accepts: either the constructor form above,
 * or a `defineGrain`/`defineReducerGrain` definition, which carries its own
 * interface and so needs nothing alongside it.
 */
export type TestGrainSpec = GrainRegistrationSpec | Registrable;

export interface TestClusterOptions {
  /** Number of silos to start with (Orleans TestClusterBuilder defaults to 2). */
  initialSilos?: number;
  clusterId?: string;
  /**
   * Stable logical service identity (Orleans `ServiceId`), distinct from
   * `clusterId`. Defaults to `clusterId` when omitted; persists across
   * `restartSilo`/`startAdditionalSilo` since it comes from cluster-level
   * config, not per-silo state.
   */
  serviceId?: string;
  /** Grains registered on every silo. */
  grains?: ReadonlyArray<TestGrainSpec>;
  /** Clock injected into every silo; pass a `FakeTimeProvider` for determinism. */
  time?: TimeProvider;
  /**
   * Whether silos are built with transaction support. Defaults to `true`
   * (every silo gets a shared in-memory transactional-storage provider); pass
   * `false` to build a cluster with no transactional storage at all, so a
   * `[Transaction]`-style grain call throws `TransactionsDisabledError`
   * (Orleans: a cluster whose `SiloHostConfigurator` never calls
   * `.UseTransactions()`).
   */
  transactions?: boolean;
  /**
   * Whether silos are built with a reminder service. Defaults to `true` (every
   * silo gets the cluster-wide `reminderTable`); pass `false` to build silos
   * with no reminder service at all, so a grain calling
   * `runtime.registerReminder` gets "reminders are not configured on this
   * silo" (Orleans: a test silo whose configurator never calls
   * `UseInMemoryReminderService()`). `reminderTable` still exists on the
   * cluster and stays empty; a silo needing its OWN table can still install one
   * from `configureSilo` via `builder.useReminders(table)`.
   */
  reminders?: boolean;
  /**
   * Per-silo overrides, applied after the defaults. `cluster` is the
   * `TestCluster` under construction — use `cluster.streamProvider(name)` to
   * register a named `MemoryStreamProvider` shared cluster-wide (the way
   * `storage`/`reminderTable` already are) instead of a fresh per-silo one.
   */
  configureSilo?: (
    builder: SiloBuilder,
    silo: { index: number; address: SiloAddress },
    cluster: TestCluster,
  ) => void;
  /**
   * Static metadata a silo advertises via membership (e.g. `{ role: "worker" }`),
   * consulted by metadata-aware placement (Orleans role-based placement). Return
   * `undefined` for a silo that advertises no metadata.
   */
  siloMetadata?: (silo: {
    index: number;
    address: SiloAddress;
  }) => Readonly<Record<string, string>> | undefined;
  /**
   * Silo-wide default per-method response timeout, forwarded to every silo
   * (Orleans `[ResponseTimeout]` default). Off by default: pass a
   * `FakeTimeProvider` via `time` to drive it deterministically in tests.
   */
  defaultResponseTimeout?: Duration;
  /** Load-shedding config applied to every silo in this cluster (Orleans `Configure<LoadSheddingOptions>`). */
  loadShedding?: Partial<LoadSheddingOptions>;
  /** Injectable RNG forwarded to every silo, for deterministic placement in tests. */
  random?: () => number;
  /** Idle-deactivation threshold for grains without their own `collectionAgeSeconds`, forwarded to every silo. */
  collectionAgeSeconds?: number;
  /**
   * Per-grain-type idle-deactivation ages forwarded to every silo (Orleans'
   * `GrainCollectionOptions.ClassSpecificCollectionAge`). Applies only to a type
   * that declares NO `@grain({ collectionAgeSeconds })` of its own: as in
   * Orleans, the grain class's own age wins over this map. A silo needing a
   * DIFFERENT map from its peers must be built with `createSilo` directly; this
   * cluster configures all of its silos alike.
   */
  classSpecificCollectionAgeSeconds?: Readonly<Record<string, number>>;
  /**
   * Default placement strategy for grain types declaring no `placement`,
   * forwarded to every silo (Orleans' `PlacementStrategy` DI singleton).
   * Unset, each silo keeps `RandomPlacement`.
   */
  defaultPlacementStrategy?: PlacementStrategy;
  /** How often the idle-collection sweep runs on every silo (defaults to 60s). */
  collectionIntervalSeconds?: number;
  /**
   * The shared transport network silos are built on. Defaults to a plain
   * `InProcessNetwork`; pass a subclass (e.g. one that counts messages) for
   * tests that need to observe traffic on the wire.
   */
  network?: InProcessNetwork;
}

/**
 * Distinguishes the client endpoints of clusters sharing one
 * `InProcessNetwork`, since an endpoint string is the network's map key.
 */
let nextClientIndex = 0;

export interface TestSiloHandle {
  readonly index: number;
  readonly address: SiloAddress;
  readonly host: SiloHost;
}

/**
 * An in-process multi-silo cluster for tests — the analogue of
 * `Orleans.TestingHost.TestCluster`. Silos share one `InProcessNetwork`, a
 * mutable membership view, and one set of memory providers (so grain state,
 * reminders, journals and jobs survive a silo's death the way a durable
 * backend would); ported functional tests only supply grains.
 */
export class TestCluster {
  private live: InternalSilo[] = [];
  private nextIndex = 0;
  private disposed = false;
  // One membership authority for the whole cluster (silos see it through
  // per-silo `localSilo()` views) so every silo agrees on the view version.
  private shared: StaticMembershipService | undefined;

  // Cluster-wide "durable" backends, shared by every silo.
  readonly storage = new MemoryGrainStorage();
  readonly reminderTable = new MemoryReminderTable();
  readonly transactionalStorage = new MemoryTransactionalStorage();
  readonly journalStorage = new MemoryJournalStorage();
  readonly jobShardStore = new MemoryJobShardStore();
  // Named `MemoryStreamProvider`s, shared cluster-wide like the backends
  // above: a producer and consumer on different silos publish/subscribe into
  // the same instance instead of two independent ones that never see each
  // other (`SiloBuilder.useMemoryStreams` otherwise constructs a fresh
  // provider per call).
  private readonly memoryStreamProviders = new Map<string, MemoryStreamProvider>();
  // The cluster client, connected on first access — see `client`.
  private connectedClient: Promise<ClientNode> | undefined;

  private constructor(
    private readonly options: TestClusterOptions,
    readonly network: InProcessNetwork,
  ) {}

  static async start(options: TestClusterOptions = {}): Promise<TestCluster> {
    const cluster = new TestCluster(options, options.network ?? new InProcessNetwork());
    const count = options.initialSilos ?? 2;
    for (let i = 0; i < count; i += 1) cluster.buildSilo();
    await Promise.all(cluster.live.map((s) => s.host.start()));
    return cluster;
  }

  get silos(): ReadonlyArray<TestSiloHandle> {
    return this.live;
  }

  /**
   * The first live silo, and the one `getGrain`/`getStreamProvider` route
   * through. A call made this way is issued BY that silo — it traverses the
   * silo's own outgoing call filters, which a call from outside the cluster
   * does not. Where that distinction matters (Orleans' `TestCluster.Client`
   * is a client, not a silo), use `client` instead.
   */
  get primary(): TestSiloHandle {
    const first = this.live[0];
    if (first === undefined) throw new Error("TestCluster has no live silos");
    return first;
  }

  /** The cluster id every silo (and any client joining it) shares. */
  get clusterId(): string {
    return this.options.clusterId ?? "test-cluster";
  }

  /** The configured service id every silo shares (Orleans `ServiceId`). */
  get serviceId(): string {
    return this.options.serviceId ?? this.clusterId;
  }

  /** Reads the active ServiceId from a running silo (Orleans test-hooks `GetServiceId()`). */
  getServiceId(handle: TestSiloHandle): string {
    return handle.host.serviceId;
  }

  getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: KeyTypeOf<K>): T {
    return this.primary.host.getGrain(def, key);
  }

  /**
   * The cluster client — Orleans' `TestCluster.Client` / `TestCluster.GrainFactory`:
   * a `ClientNode` OUTSIDE every silo, joined to this cluster's network with
   * the silos themselves as gateways, hosting the same grain registrations
   * `TestClusterOptions.grains` gave them (so `getGrain` resolves), plus
   * `createObjectReference` for observer callbacks.
   *
   * It is a genuinely different caller from `getGrain`, and that is the point:
   * a call through a silo runs that silo's outgoing call filters, a call
   * through this client runs none of them. Nothing else on `TestCluster` is
   * routed through it — `getGrain` and `primary` still go through the silo —
   * so adopting it is per-call and never changes an existing test.
   *
   * Connecting is asynchronous, so the accessor yields a promise: `await
   * cluster.client`. It is created on FIRST access (Orleans creates it during
   * `Deploy`), because a connected client registers in the cluster's client
   * directory and opens a silo connection: a cluster that never touches it
   * sees no traffic it would not otherwise have seen, which is what keeps
   * message-counting tests built on `TestClusterOptions.network` intact.
   * `dispose()` closes it, before stopping any silo.
   *
   * Grains registered only by a `configureSilo` override are NOT on it; put a
   * registration in `TestClusterOptions.grains` for the client to address it.
   */
  get client(): Promise<ClientNode> {
    return (this.connectedClient ??= this.connectClient());
  }

  private async connectClient(): Promise<ClientNode> {
    // The same error `primary` raises: a cluster with no silos left (or one
    // already disposed) has nothing for a client to talk to.
    const membership = this.shared;
    if (membership === undefined || this.live.length === 0) {
      throw new Error("TestCluster has no live silos");
    }
    const index = nextClientIndex;
    nextClientIndex += 1;
    const local = new SiloAddress(
      `test-client-${index}`,
      `uid-client-${index}`,
      `test-client-${index}:22222`,
    );
    const client = createClient({
      clusterId: this.clusterId,
      local,
      transport: new InProcessTransport(this.network, this.clusterId),
      // Every live silo is a gateway, the way Orleans' test client shares the
      // silos' membership table: a cluster that kills or restarts its primary
      // keeps a usable client instead of one pinned to a dead endpoint.
      gateways: membershipGatewayProvider(membership),
    }).registerGrains(this.options.grains ?? []);
    await client.connect();
    return client;
  }

  /**
   * The cluster-wide `MemoryStreamProvider` registered under `name`,
   * creating it on first use. Every silo's `useMemoryStreams(name, ..., ...)`
   * call (the default chain, and any `configureSilo` override that asks for
   * this same name) reuses the identical instance, so producer and consumer
   * activations landing on different silos still share one provider.
   */
  streamProvider(name = "default"): MemoryStreamProvider {
    let provider = this.memoryStreamProviders.get(name);
    if (provider === undefined) {
      provider = new MemoryStreamProvider(name);
      this.memoryStreamProviders.set(name, provider);
    }
    return provider;
  }

  /**
   * The named stream provider hosted by the primary silo (Orleans
   * `fixture.Client.GetStreamProvider(name)`, adapted: ported tests have no
   * separate client-process gateway, so they reach a provider through the
   * primary silo directly, same as `getGrain`). `undefined` if no provider by
   * that name is configured.
   */
  getStreamProvider(name?: string): StreamProvider | undefined {
    return this.primary.host.getStreamProvider(name);
  }

  /**
   * A `GrainCancellationTokenSource` whose `canceller` reaches any recorded
   * target grain's `ICancellationSourcesExtension`, wherever it lives: the
   * dispatcher routes the extension call to the hosting silo the same way it
   * routes an ordinary grain call, so this works whether the target
   * activated on the primary silo or a peer. Cancels are issued `from` the
   * given silo (defaults to `primary`) — pass the same silo a test calls the
   * target grain from to observe same-silo (no-wire) vs. cross-silo
   * propagation.
   */
  newCancellationTokenSource(from: TestSiloHandle = this.primary): GrainCancellationTokenSource {
    return new GrainCancellationTokenSource(async (target: GrainId, tokenId: string) => {
      const ext = from.host.getExtensionReference(ICancellationSourcesExtension, target);
      await ext.cancelRemoteToken(tokenId);
    });
  }

  async startAdditionalSilo(): Promise<TestSiloHandle> {
    const silo = this.buildSilo();
    await silo.host.start();
    return silo;
  }

  /** Graceful shutdown: the silo drains and departs the membership view. */
  async stopSilo(handle: TestSiloHandle): Promise<void> {
    const silo = this.take(handle);
    await silo.host.stop();
    this.shared?.removeSilo(silo.address);
  }

  /**
   * Abrupt failure: detach the silo from the network and declare it dead to the
   * survivors, without running any shutdown hooks — the in-process stand-in for
   * a killed pod.
   */
  async killSilo(handle: TestSiloHandle): Promise<void> {
    const silo = this.take(handle);
    this.network.unregister(silo.address);
    this.shared?.removeSilo(silo.address);
  }

  /** Kill-then-replace: a fresh silo joins with a new uid, like a restarted pod. */
  async restartSilo(handle: TestSiloHandle): Promise<TestSiloHandle> {
    await this.killSilo(handle);
    return this.startAdditionalSilo();
  }

  /**
   * Stop every live silo in a deterministic order — mirroring `stopSilo`'s
   * stop-then-`removeSilo` sequencing per silo — rather than tearing them all
   * down concurrently. A bare `Promise.all` would race: a silo mid-`stop()`
   * runs `onDeactivate` hooks that can make cross-silo calls (the
   * `ActivateDeactivateWatcherGrain` pattern), and if every silo's transport
   * were closing at once, that call could land on a peer whose listener is
   * already gone. Stopping one at a time, and updating membership between
   * each, keeps the remaining silos reachable until it is their own turn.
   *
   * The cluster client (if one was ever created) is closed FIRST, matching
   * Orleans' `StopAllSilosAsync`, which calls `StopClusterClientAsync` before
   * stopping any silo: a client outliving its gateway leaves a listener on the
   * network with nothing to serve it.
   */
  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    const client = this.connectedClient;
    this.connectedClient = undefined;
    if (client !== undefined) {
      await client.then((c) => c.close()).catch(() => undefined);
    }
    const remaining = this.live;
    this.live = [];
    for (const silo of remaining) {
      await silo.host.stop().catch(() => undefined);
      this.shared?.removeSilo(silo.address);
    }
  }

  private take(handle: TestSiloHandle): InternalSilo {
    const silo = this.live.find((s) => s === handle);
    if (silo === undefined) throw new Error(`silo ${handle.address.podName} is not live`);
    this.live = this.live.filter((s) => s !== silo);
    return silo;
  }

  private buildSilo(): InternalSilo {
    const index = this.nextIndex;
    this.nextIndex += 1;
    const address = new SiloAddress(
      `test-silo-${index}`,
      `uid-${index}`,
      `test-silo-${index}:11111`,
    );
    const metadata = this.options.siloMetadata?.({ index, address });
    if (this.shared === undefined) {
      this.shared = new StaticMembershipService(
        address,
        [address],
        metadata !== undefined ? () => metadata : undefined,
      );
    } else {
      this.shared.addSilo(address, "active", metadata);
    }
    const membership = new MembershipView(this.shared, address);
    const builder = createSilo({
      clusterId: this.options.clusterId ?? "test-cluster",
      serviceId: this.options.serviceId ?? this.options.clusterId ?? "test-cluster",
      local: address,
      ...(this.options.time !== undefined ? { time: this.options.time } : {}),
      ...(metadata !== undefined ? { metadata } : {}),
      ...(this.options.defaultResponseTimeout !== undefined
        ? { defaultResponseTimeout: this.options.defaultResponseTimeout }
        : {}),
      ...(this.options.loadShedding !== undefined
        ? { loadShedding: this.options.loadShedding }
        : {}),
      ...(this.options.random !== undefined ? { random: this.options.random } : {}),
      ...(this.options.collectionAgeSeconds !== undefined
        ? { collectionAgeSeconds: this.options.collectionAgeSeconds }
        : {}),
      ...(this.options.classSpecificCollectionAgeSeconds !== undefined
        ? { classSpecificCollectionAgeSeconds: this.options.classSpecificCollectionAgeSeconds }
        : {}),
      ...(this.options.defaultPlacementStrategy !== undefined
        ? { defaultPlacementStrategy: this.options.defaultPlacementStrategy }
        : {}),
      ...(this.options.collectionIntervalSeconds !== undefined
        ? { collectionIntervalSeconds: this.options.collectionIntervalSeconds }
        : {}),
    })
      .useMembership(membership)
      .useInProcessTransport(this.network)
      .useMemoryStorage(this.storage)
      .useMemoryStreams("default", undefined, this.streamProvider("default"))
      .useBroadcastChannels()
      .useMemoryJournaling(this.journalStorage)
      .useMemoryDurableJobs(this.jobShardStore);
    // Orleans expresses "this deployment has no reminder service" by simply not
    // calling `UseInMemoryReminderService()`, so `reminders: false` must leave
    // the builder's table undefined rather than install a no-op one — that is
    // what makes the silo build with no `reminderRegistry` at all.
    if (this.options.reminders ?? true) {
      builder.useReminders(this.reminderTable);
    }
    if (this.options.transactions ?? true) {
      builder.useMemoryTransactionalStorage(this.transactionalStorage);
    } else {
      builder.disableTransactions();
    }
    for (const spec of this.options.grains ?? []) {
      if ("ctor" in spec) builder.registerGrain(spec.ctor, { interfaces: spec.interfaces });
      else builder.registerGrain(spec);
    }
    this.options.configureSilo?.(builder, { index, address }, this);
    const silo: InternalSilo = { index, address, membership, host: builder.build() };
    this.live.push(silo);
    return silo;
  }
}

interface InternalSilo extends TestSiloHandle {
  readonly membership: MembershipService;
}

/** Reports each silo's own `localSilo()` while sharing one membership view. */
class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}

  current(): MembershipSnapshot {
    return this.shared.current();
  }

  updates(): AsyncIterableIterator<MembershipSnapshot> {
    return this.shared.updates();
  }

  localSilo(): SiloAddress {
    return this.local;
  }
}
