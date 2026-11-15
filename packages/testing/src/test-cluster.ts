import type { Duration } from "@thresh/core/duration";
import { GrainCancellationTokenSource } from "@thresh/core/grain-cancellation-token";
import type { GrainRegistrationSpec } from "@thresh/core/grain-registration-spec";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import type { Registrable } from "@thresh/core/grain-registration";
import type { KeyTypeOf } from "@thresh/core/key-kinds";
import { noopLogger, type Logger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import type { StreamProvider } from "@thresh/core/stream";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import {
  FaultInjectingTransport,
  NetworkFaults,
} from "@thresh/messaging/fault-injecting-transport";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ICancellationSourcesExtension } from "@thresh/runtime/cancellation-extension";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import type { StorageBackend } from "@thresh/hosting/storage-backend";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { MemoryStreamProvider } from "@thresh/streams/memory-stream-provider";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import type { LoadSheddingOptions } from "@thresh/runtime/load-shedding";
import type { LocalCallSemantics } from "@thresh/runtime/local-call-semantics-dispatcher";
import type { PlacementStrategy } from "@thresh/runtime/placement/placement-strategy";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";
import { SiloBuilder, type SiloConfig } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { createClient, type ClientNode } from "@thresh/client/client-node";
import { membershipGatewayProvider } from "@thresh/client/gateway-provider";
import {
  ringSilos,
  type MembershipService,
  type MembershipSnapshot,
} from "@thresh/core/membership";
import { CrashableTransport, SharedMembershipView, SiloClock } from "@thresh/testing/silo-crash";
import { KubernetesControls, SILO_METADATA_LABEL_PREFIX } from "@thresh/testing/test-kubernetes";

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
  /**
   * The logger every silo in the cluster is built with. Defaults to `noopLogger`, so a suite's
   * output is not filled with the runtime's own warnings; pass a recording logger to assert on them.
   */
  logger?: Logger;
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
  /** Transactions tunables forwarded to every silo (see `SiloConfig.transactionsOptions`). */
  transactionsOptions?: Partial<TransactionsOptions>;
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
   * Keep grain state, journals and reminders in this backend instead of the
   * cluster's shared in-memory providers: every silo calls
   * `useStorageBackend(storageBackend)` in place of the memory defaults. A
   * DURABLE backend lets a test prove recovery across a WHOLE-cluster restart
   * (dispose one cluster, start another with the same `serviceId` over the same
   * backend), which shared memory cannot. `storage`, `journalStorage` and
   * `reminderTable` on the cluster stay unused.
   */
  storageBackend?: StorageBackend;
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
  /**
   * How long a caller waits for a cross-silo reply, and so the time-to-live
   * every request carries, forwarded to every silo (Orleans
   * `SiloMessagingOptions.ResponseTimeout`; see `SiloConfig.callTimeout`).
   * Defaults to 30s.
   */
  callTimeout?: Duration;
  /**
   * How long a silo's directory waits for members that do not answer a range recovery before it
   * unlocks the range without them, forwarded to every silo (`SiloConfig.directoryRecoveryPatience`).
   * Unset, a range stays locked for as long as such a member is listed.
   */
  directoryRecoveryPatience?: Duration;
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
   * How long an activation's state load and `onActivate` may take before it fails
   * (`SiloConfig.activationTimeout`, Orleans `CollectionOptions.ActivationTimeout`). On the
   * injected `time`. Defaults to 30s.
   */
  activationTimeout?: Duration | false;
  /**
   * How long a turn may run before a request waiting behind it has the activation deactivated
   * as stuck (`SiloConfig.scheduling.maxRequestProcessingTime`, Orleans
   * `SiloMessagingOptions.MaxRequestProcessingTime`). On the injected `time`. Defaults to 2h.
   */
  maxRequestProcessingTime?: Duration;
  /**
   * How often every silo pushes its load snapshot to its peers (Orleans
   * `DeploymentLoadPublisherOptions`, defaults to 1s). Lower it in a test that
   * needs cross-silo load visibility without waiting a full period, or pass
   * `0` to disable the periodic push and rely only on the test hooks'
   * forced pushes.
   */
  loadPublishIntervalMs?: number;
  /**
   * How long a disconnected client stays registered on its gateway before
   * being dropped, forwarded to every silo (Orleans
   * `SiloMessagingOptions.ClientDropTimeout`, defaults to 1 minute).
   */
  clientDropTimeoutMs?: number;
  /**
   * How often every silo republishes its locally connected clients to its
   * peers, on top of the immediate republish a membership change triggers
   * (Orleans `SiloMessagingOptions.ClientRegistrationRefresh`, defaults to 5
   * minutes).
   */
  clientDirectoryRefreshMs?: number;
  /**
   * The shared transport network silos are built on. Defaults to a plain
   * `InProcessNetwork`; pass a subclass (e.g. one that counts messages) for
   * tests that need to observe traffic on the wire.
   */
  network?: InProcessNetwork;
  /**
   * The faults every silo's transport injects (see `TestCluster.faults`). Defaults to a fresh
   * `NetworkFaults` on the cluster's clock with nothing set; pass one to arm a fault before the
   * cluster starts, e.g. a refused connect that must hold from the first dial.
   */
  faults?: NetworkFaults;
  /**
   * Which membership the silos run on.
   *
   * `"static"` (the default) gives every silo a window onto ONE shared membership object: a
   * view change reaches every silo at once with the same version, and a stopped or killed silo
   * leaves every view in the same instant. That is a property of this fixture, not of
   * production, where each silo follows its own watch.
   *
   * `"kubernetes"` is the production path: each silo runs `useKubernetesMembership` over its
   * OWN watch of one fake EndpointSlice API server, which the test drives through
   * `TestCluster.kubernetes` — readiness, two-step departure, a lagging watch, a forced relist.
   * Views then change silo by silo and can differ; `viewOf` shows each silo's. A silo joins as
   * a pod does: listed not ready (joining, outside the ring while any peer is active) as it
   * starts, then marked ready by the kubelet once its own readiness passes — or held not ready
   * (`startAdditionalSilo({ holdReadiness: true })`). A graceful stop marks its pod terminating
   * (draining) before the silo drains and removes it after, and a kill leaves its pod listed,
   * ready, until the test takes it away (`kubernetes.depart`), as after a real crash.
   */
  membership?: "static" | "kubernetes";
  /**
   * What every silo does to a same-silo call's arguments, result and thrown error
   * (`SiloConfig.localCallSemantics`). `"wire"` by default: they make the serializer round trip,
   * so a suite that happens to land caller and callee together still sees no aliasing, class
   * instances as the wire delivers them and error classes as the wire rebuilds them. `"copy"`
   * gives the same through the deep copier (sharing values marked `immutable`); pass
   * `"reference"` for a test that pins local-only behaviour.
   */
  localCallSemantics?: LocalCallSemantics;
}

/** How `TestCluster.startAdditionalSilo` brings a silo up. */
export interface StartSiloOptions {
  /**
   * Under Kubernetes membership, leave the silo's pod not ready after it starts instead of letting
   * the kubelet mark it ready: it stays a joining member, outside the ring while any peer is
   * active, until the test flips it.
   */
  readonly holdReadiness?: boolean;
}

/** One silo's membership as it stands: the view its membership holds, and the one its node applied. */
export interface SiloView {
  readonly membership: MembershipSnapshot;
  readonly appliedVersion: number;
}

export interface TestSiloHandle {
  readonly index: number;
  readonly address: SiloAddress;
  readonly host: SiloHost;
}

/**
 * An in-process multi-silo cluster for tests — the analogue of
 * `Orleans.TestingHost.TestCluster`. Silos share one `InProcessNetwork` (each
 * behind its own fault-injecting transport, see `faults`), a membership (one
 * shared static view by default, or each silo's own Kubernetes watch, see
 * `TestClusterOptions.membership`), and one set of memory providers (so grain
 * state, reminders, journals and jobs survive a silo's death the way a durable
 * backend would); ported functional tests only supply grains.
 */
export class TestCluster {
  private live: InternalSilo[] = [];
  private nextIndex = 0;
  private disposed = false;
  // One membership authority for the whole cluster (silos see it through
  // per-silo `localSilo()` views) so every silo agrees on the view version.
  // That agreement is a property of this fixture, not of membership in general:
  // each production silo numbers its views from its own Kubernetes watch, so
  // there identical versions denote unrelated views (issue #72). Tests rely on
  // version equality meaning view equality — e.g. the directory's `staleView`
  // guard — and must keep sharing one service to do so.
  private shared: StaticMembershipService | undefined;
  // Set only for `membership: "kubernetes"`.
  private readonly k8s: KubernetesControls | undefined;

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

  /**
   * Network faults between this cluster's silos: drop, delay, refuse connects, partition and
   * heal, by direct call or explicit schedule. Every silo's transport is a
   * `FaultInjectingTransport` over this one object, applying faults to what that silo sends.
   */
  readonly faults: NetworkFaults;

  private constructor(
    private readonly options: TestClusterOptions,
    readonly network: InProcessNetwork,
  ) {
    this.faults = options.faults ?? new NetworkFaults(options.time);
    this.k8s =
      options.membership === "kubernetes"
        ? new KubernetesControls(() =>
            this.live.every((s) => {
              const view = this.viewOf(s);
              return view.appliedVersion === view.membership.version;
            }),
          )
        : undefined;
  }

  static async start(options: TestClusterOptions = {}): Promise<TestCluster> {
    const cluster = new TestCluster(options, options.network ?? new InProcessNetwork());
    const count = options.initialSilos ?? 2;
    if (cluster.k8s !== undefined) {
      // Pods come up one at a time, each joining the cluster the ones before it formed.
      for (let i = 0; i < count; i += 1) await cluster.startAdditionalSilo();
      return cluster;
    }
    for (let i = 0; i < count; i += 1) cluster.buildSilo();
    await Promise.all(cluster.live.map((s) => s.host.start()));
    return cluster;
  }

  /**
   * The fake Kubernetes API server and the controls over it, for a cluster started with
   * `membership: "kubernetes"`. Throws on the default static membership, which has no
   * Kubernetes to drive.
   */
  get kubernetes(): KubernetesControls {
    if (this.k8s === undefined) {
      throw new Error(
        'TestCluster.kubernetes needs a cluster started with membership: "kubernetes"',
      );
    }
    return this.k8s;
  }

  /**
   * `silo`'s membership view and the view version its node has applied — the pair that tells
   * a silo whose watch lags (an older view) from one still applying a view it has (an applied
   * version behind its own view's).
   */
  viewOf(silo: TestSiloHandle): SiloView {
    // `SiloHost` exposes neither its membership nor its node; read them the way the hosting
    // tests do. Introspection only.
    const parts = (
      silo.host as unknown as {
        parts: { membership: MembershipService; node: { readonly appliedViewVersion: number } };
      }
    ).parts;
    return {
      membership: parts.membership.current(),
      appliedVersion: parts.node.appliedViewVersion,
    };
  }

  get silos(): ReadonlyArray<TestSiloHandle> {
    return this.live;
  }

  /**
   * The grain directory's integrity check on every live silo (Orleans
   * `IGrainDirectoryTestHooks.CheckIntegrityAsync`, which the upstream resilience tests run per
   * silo): no partition holds an entry outside the ranges it owns, and every registered live
   * activation in the cluster has exactly its own entry at its owner — so no grain is registered
   * as active in two places. Under Kubernetes membership it first lets every silo apply the view
   * its own watch holds (`kubernetes.settle`). Throws, listing every violation, if any is found.
   */
  async checkIntegrity(): Promise<void> {
    if (this.k8s !== undefined) await this.k8s.settle();
    const violations: string[] = [];
    for (const silo of this.live) {
      const node = (
        silo.host as unknown as {
          parts: { node: { checkDirectoryIntegrity(): Promise<Array<{ detail: string }>> } };
        }
      ).parts.node;
      for (const violation of await node.checkDirectoryIntegrity()) {
        violations.push(`${silo.address.podName}: ${violation.detail}`);
      }
    }
    if (violations.length > 0) {
      throw new Error(`grain directory integrity violated:\n  ${violations.join("\n  ")}`);
    }
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
    if ((membership === undefined && this.k8s === undefined) || this.live.length === 0) {
      throw new Error("TestCluster has no live silos");
    }
    const client = createClient({
      clusterId: this.clusterId,
      transport: new InProcessTransport(this.network, this.clusterId),
      // Every live silo is a gateway, the way Orleans' test client shares the
      // silos' membership table: a cluster that kills or restarts its primary
      // keeps a usable client instead of one pinned to a dead endpoint.
      gateways:
        membership !== undefined
          ? membershipGatewayProvider(membership)
          : { getGateways: () => ringSilos(this.viewOf(this.primary).membership) },
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

  /**
   * Start one more silo. Under Kubernetes membership its pod is listed not ready as it starts
   * (a joining member, outside the ring while any peer is active), and the kubelet marks it ready
   * once the silo's own readiness passes — unless `holdReadiness`, which leaves the pod not ready
   * until the test flips it (`kubernetes.setReady`, or `kubernetes.kubelet`). Ignored under the
   * default static membership, where a silo is active from the start.
   */
  async startAdditionalSilo(options: StartSiloOptions = {}): Promise<TestSiloHandle> {
    const silo = this.buildSilo();
    await silo.host.start();
    if (this.k8s !== undefined && options.holdReadiness !== true) await this.k8s.kubelet(silo);
    return silo;
  }

  /**
   * Start `count` more silos at once, as a Deployment rolls out several pods together: every pod
   * is listed not ready before any silo starts, the silos start concurrently, and the kubelet
   * marks each ready as its own readiness passes (unless `holdReadiness`). On a cluster with no
   * ready silo this is a cold start: the ring is the joining silos until the first is ready.
   */
  async startAdditionalSilos(
    count: number,
    options: StartSiloOptions = {},
  ): Promise<TestSiloHandle[]> {
    const silos = Array.from({ length: count }, () => this.buildSilo());
    await Promise.all(
      silos.map(async (silo) => {
        await silo.host.start();
        if (this.k8s !== undefined && options.holdReadiness !== true) await this.k8s.kubelet(silo);
      }),
    );
    return silos;
  }

  /**
   * Migrate `grainId`'s live activation to `targetSilo`, with its migration state (Orleans
   * `TestCluster.MigrateAsync`). Resolves `true` once the target accepted it; `false` when no
   * silo holds an activation of it.
   */
  async migrate(grainId: GrainId, targetSilo: SiloAddress): Promise<boolean> {
    const source = this.live.find((s) => s.host.isActive(grainId));
    if (source === undefined) return false;
    return source.host.migrate(grainId, targetSilo);
  }

  /** Graceful shutdown: the silo drains and departs the membership view. */
  async stopSilo(handle: TestSiloHandle): Promise<void> {
    const silo = this.take(handle);
    await this.stopGracefully(silo);
  }

  /** Drain `silo` and take it out of membership, the way its kind of membership does. */
  private async stopGracefully(silo: InternalSilo): Promise<void> {
    if (this.k8s === undefined) {
      await silo.host.stop();
      this.shared?.removeSilo(silo.address);
      return;
    }
    // A terminating pod's endpoint turns not ready and terminating first, the silo drains, and
    // its endpoint goes after.
    await this.k8s.terminate(silo);
    await silo.host.stop();
    await this.k8s.removePod(silo);
  }

  /**
   * Abrupt failure — the in-process stand-in for a crashed pod (Orleans
   * `KillSiloAsync`, an ungraceful stop). The silo is declared dead to the
   * survivors and its process is cut off: its transport is gone (peers dialling
   * it are refused and nothing it sends arrives), every timer it armed is
   * cancelled and none fires again (grain timers, reminder ticks, durable-job
   * polls, idle collection), its pulling-stream agents stop, its view of
   * membership freezes (so it runs no further ownership hooks). No shutdown
   * code runs: no drain, no deactivation hook, no release of anything it held.
   * Its activations stay in memory, unreachable. One thing an in-process
   * stand-in cannot do is preempt JavaScript already running: a turn that was
   * mid-flight finishes its own code (its outgoing calls fail; nothing new
   * starts). `stopSilo` remains the graceful path.
   */
  async killSilo(handle: TestSiloHandle): Promise<void> {
    const silo = this.take(handle);
    // The listener closes synchronously inside `crash()`, and the view drops the silo in the
    // same tick, so no peer ever sees it listed but unreachable through this harness's doing.
    const crashed = silo.crash();
    this.shared?.removeSilo(silo.address);
    await crashed;
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
   * stopping any silo: a client outliving its gateway would otherwise start
   * failing every call the instant its gateway stops mid-close.
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
      await this.stopGracefully(silo).catch(() => undefined);
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
    if (this.k8s !== undefined) return this.buildKubernetesSilo(index);
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
    const membership = new SharedMembershipView(this.shared, address);
    return this.buildSiloOn(
      index,
      address,
      metadata,
      (builder) => builder.useMembership(membership),
      () => membership.freeze(),
    );
  }

  private buildKubernetesSilo(index: number): InternalSilo {
    const k8s = this.kubernetes;
    const { address, metadata, watch } = k8s.addPod(index, (address) =>
      this.options.siloMetadata?.({ index, address }),
    );
    const silo = this.buildSiloOn(
      index,
      address,
      metadata,
      (builder) =>
        builder.useKubernetesMembership(watch, {
          portName: "silo",
          metadataLabelPrefix: SILO_METADATA_LABEL_PREFIX,
        }),
      // A crashed pod's watch is gone with its process; its endpoint stays until Kubernetes acts.
      () => k8s.disconnect({ address }),
    );
    return silo;
  }

  private buildSiloOn(
    index: number,
    address: SiloAddress,
    metadata: Readonly<Record<string, string>> | undefined,
    useMembership: (builder: SiloBuilder) => void,
    stopFollowingMembership: () => void,
  ): InternalSilo {
    const clock = new SiloClock(this.options.time ?? systemTimeProvider);
    const transport = new CrashableTransport(
      new FaultInjectingTransport(
        new InProcessTransport(this.network, this.clusterId),
        this.faults,
      ),
    );
    const builder = new TestSiloBuilder({
      clusterId: this.options.clusterId ?? "test-cluster",
      serviceId: this.options.serviceId ?? this.options.clusterId ?? "test-cluster",
      local: address,
      logger: this.options.logger ?? noopLogger,
      time: clock,
      // Nothing outside the process routes to these silos, so a drain has no one to wait for.
      gracefulShutdownMs: 0,
      ...(metadata !== undefined ? { metadata } : {}),
      ...(this.options.defaultResponseTimeout !== undefined
        ? { defaultResponseTimeout: this.options.defaultResponseTimeout }
        : {}),
      ...(this.options.callTimeout !== undefined ? { callTimeout: this.options.callTimeout } : {}),
      ...(this.options.directoryRecoveryPatience !== undefined
        ? { directoryRecoveryPatience: this.options.directoryRecoveryPatience }
        : {}),
      ...(this.options.transactionsOptions !== undefined
        ? { transactionsOptions: this.options.transactionsOptions }
        : {}),
      ...(this.options.loadShedding !== undefined
        ? { loadShedding: this.options.loadShedding }
        : {}),
      testHooks: true,
      ...(this.options.random !== undefined ? { random: this.options.random } : {}),
      // "wire" unless a test chooses: a same-silo call shows what a cross-silo call would.
      localCallSemantics: this.options.localCallSemantics ?? "wire",
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
      ...(this.options.activationTimeout !== undefined
        ? { activationTimeout: this.options.activationTimeout }
        : {}),
      ...(this.options.maxRequestProcessingTime !== undefined
        ? { scheduling: { maxRequestProcessingTime: this.options.maxRequestProcessingTime } }
        : {}),
      ...(this.options.loadPublishIntervalMs !== undefined
        ? { loadPublishIntervalMs: this.options.loadPublishIntervalMs }
        : {}),
      ...(this.options.clientDropTimeoutMs !== undefined
        ? { clientDropTimeoutMs: this.options.clientDropTimeoutMs }
        : {}),
      ...(this.options.clientDirectoryRefreshMs !== undefined
        ? { clientDirectoryRefreshMs: this.options.clientDirectoryRefreshMs }
        : {}),
    });
    useMembership(builder);
    builder
      .useTransport(transport)
      .useMemoryStreams("default", undefined, this.streamProvider("default"))
      .useBroadcastChannels()
      .useMemoryDurableJobs(this.jobShardStore)
      // Every in-memory store below is ONE instance shared by all of this cluster's silos.
      .allowSharedInMemoryState();
    const backend = this.options.storageBackend;
    if (backend === undefined) {
      builder.useMemoryStorage(this.storage).useMemoryJournaling(this.journalStorage);
    } else {
      builder.useStorageBackend(backend);
    }
    // Orleans expresses "this deployment has no reminder service" by simply not
    // calling `UseInMemoryReminderService()`, so `reminders: false` must leave
    // the builder's table undefined rather than install a no-op one — that is
    // what makes the silo build with no `reminderRegistry` at all.
    if (this.options.reminders ?? true) {
      // With a backend, `useReminders()` takes the backend's table.
      if (backend === undefined) builder.useReminders(this.reminderTable);
      else builder.useReminders();
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
    const host = builder.build();
    const shared = new Set<StreamProvider>(this.memoryStreamProviders.values());
    const crash = async (): Promise<void> => {
      clock.stop();
      stopFollowingMembership();
      const transportGone = transport.crash();
      // Pulling agents poll on their own timers; stopping a provider only stops its polling. A
      // provider shared with the survivors is theirs too and keeps running.
      await Promise.all(
        builder.givenStreamProviders
          .filter((provider) => !shared.has(provider))
          .map((provider) => (provider as { stop?: () => Promise<void> }).stop?.()),
      );
      await transportGone;
    };
    const silo: InternalSilo = { index, address, host, crash };
    this.live.push(silo);
    return silo;
  }
}

interface InternalSilo extends TestSiloHandle {
  /** Cut the silo off as a crash does; see `TestCluster.killSilo`. */
  crash(): Promise<void>;
}

/** A `SiloBuilder` that remembers the stream providers it was given, so a crash can stop them. */
class TestSiloBuilder extends SiloBuilder {
  readonly givenStreamProviders: StreamProvider[] = [];

  constructor(config: SiloConfig) {
    super(config);
  }

  override addStreamProvider(name: string, provider: StreamProvider): this {
    this.givenStreamProviders.push(provider);
    return super.addStreamProvider(name, provider);
  }
}
