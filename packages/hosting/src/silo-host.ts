import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import type { KeyTypeOf } from "@thresh/core/key-kinds";
import { type Logger, noopLogger } from "@thresh/core/logger";
import type { MembershipService, MembershipSnapshot } from "@thresh/core/membership";
import type { StreamProvider } from "@thresh/core/stream";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import type { ClusterNode } from "@thresh/runtime/cluster-node";
import type { SiloLoadSheddingTestHooks } from "@thresh/runtime/load-shedding";
import type {
  ActivationRebalancerWorker,
  RebalancerReportListener,
} from "@thresh/runtime/placement/rebalancing/rebalancer-worker";
import type { RebalancingReport } from "@thresh/runtime/placement/rebalancing/rebalancing-report";
import type { Edge } from "@thresh/runtime/placement/repartitioning/edge";
import { GracefulShutdown } from "@thresh/hosting/graceful-shutdown";
import type { HealthCheck } from "@thresh/hosting/health-check";
import type { HealthServer } from "@thresh/hosting/health-server";
import type { SelfProbeWorker } from "@thresh/hosting/self-probe";

/** The reminder service the host drives (a `LocalReminderService`). */
export interface ReminderService {
  refreshOwnership(ranges: ReadonlyArray<readonly [number, number]>): Promise<void>;
  stop(): void;
}

export interface SiloHostParts {
  node: ClusterNode;
  /**
   * Stable logical service identity (Orleans `ServiceId`), carried for the
   * silo's lifetime — including across a restart, since it comes from
   * cluster-level config, not per-process state.
   */
  serviceId: string;
  health: HealthCheck;
  healthServer: HealthServer | undefined;
  healthPort: number | undefined;
  shutdown: GracefulShutdown;
  /**
   * `GracefulShutdown`'s grace period, used only when `buildSiloHost` constructs the default
   * `GracefulShutdown` itself (a caller supplying its own `shutdown` configures the grace
   * directly and this is ignored). Unset falls back to `GracefulShutdown`'s own default.
   */
  gracefulShutdownMs?: number;
  /**
   * `GracefulShutdown`'s overall stop budget, used the same way as
   * `gracefulShutdownMs` (only when `buildSiloHost` constructs the default
   * `GracefulShutdown`; ignored with a caller-supplied `shutdown`). Unset
   * falls back to `GracefulShutdown`'s own default (`DEFAULT_STOP_BUDGET_MS`).
   */
  stopBudgetMs?: number;
  membership: MembershipService;
  reminderService?: ReminderService | undefined;
  /** The elected activation-rebalancer worker, started/stopped with the host. */
  rebalancerWorker?: ActivationRebalancerWorker | undefined;
  /**
   * The self-probe liveness worker (docs/design-notes-parity-gaps.md item 9,
   * option A), started/stopped with the host. `undefined` when
   * `SiloConfig.selfProbe.enabled` is `false`. Started AFTER `node.start()`
   * (like `rebalancerWorker`) — probing before the node is even accepting
   * calls would just record spurious misses.
   */
  selfProbeWorker?: SelfProbeWorker | undefined;
  /** Run before the node starts — e.g. connect durable provider clients. */
  onStart?: ReadonlyArray<() => Promise<void>>;
  /**
   * Run on start and on every membership change with this silo's owned hash
   * ranges — e.g. take over the stream queues the ring now assigns this silo.
   */
  onOwnershipChange?: ReadonlyArray<
    (ranges: ReadonlyArray<readonly [number, number]>) => Promise<void>
  >;
  /** Run after the node drains — e.g. disconnect durable provider clients. */
  onStop?: ReadonlyArray<() => Promise<void>>;
  /**
   * Run before the node deactivates its activations (`shutdown.drain()` ->
   * `ClusterNode.stop()` -> `Catalog.deactivateAll`) — stream providers and
   * the durable-job manager stop here, matching Orleans' own ordering
   * (`PersistentStreamProviderOptions.cs:58`, `LocalDurableJobManager.cs:120`
   * both stop at `ServiceLifecycleStage.Active`, before
   * `Catalog.DeactivateAllActivations`, `Silo.cs:387-395`): otherwise they
   * keep delivering into activations for the whole grace period plus
   * deactivation, creating exactly the orphaned-activation race
   * `deactivateAll` now refuses (issue #108), then failing once the
   * transport underneath them has closed anyway.
   */
  onBeforeDeactivate?: ReadonlyArray<() => Promise<void>>;
  /**
   * Run after the node starts but before the silo is marked ready — e.g. call
   * grains from application startup code (Orleans `IStartupTask`).
   */
  startupTasks?: ReadonlyArray<() => Promise<void>>;
  /**
   * Where the host reports its own failures — the membership watch's failed view
   * updates. Unset is a no-op, like every other optional logger seam here; the
   * builder passes its `useLogging` logger through.
   */
  logger?: Logger | undefined;
}

/**
 * A started silo: the cluster node plus its health probes and drain coordinator.
 * `start` brings it online and flips readiness; `stop` drains gracefully.
 */
export class SiloHost {
  private membershipWatch: AbortController | undefined;
  private stopViewListener: (() => void) | undefined;
  /** The in-flight view reconciliation (`reconcileView`), if one is running. */
  private reconciling: Promise<void> | undefined;
  /** Set when a reconcile is requested while one runs: it goes round again. */
  private reconcileAgain = false;
  /** The applied view version the ownership hooks last ran for. */
  private hookedVersion: number | undefined;

  constructor(private readonly parts: SiloHostParts) {}

  get health(): HealthCheck {
    return this.parts.health;
  }

  /** The active ServiceId this silo is running with (Orleans test-hooks `GetServiceId()`). */
  get serviceId(): string {
    return this.parts.serviceId;
  }

  getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: KeyTypeOf<K>): T {
    return this.parts.node.getGrain(def, key);
  }

  /** See `ClusterNode.getExtensionReference`: a reference to an explicit `GrainId` under `def`. */
  getExtensionReference<T>(def: GrainInterface<T>, grainId: GrainId): T {
    return this.parts.node.getExtensionReference(def, grainId);
  }

  isActive(id: GrainId): boolean {
    return this.parts.node.isActive(id);
  }

  /** Migrate this silo's activation of `id` to `target`; `false` if this silo does not hold it. */
  migrate(id: GrainId, target: SiloAddress): Promise<boolean> {
    return this.parts.node.migrateGrain(id, target);
  }

  /** The named stream provider this silo hosts, or `undefined` if none is configured by that name. */
  getStreamProvider(name?: string): StreamProvider | undefined {
    return this.parts.node.streamProvider(name);
  }

  /**
   * This silo's `IGrainDirectory` (Orleans
   * `Services.GetRequiredService<GrainDirectoryResolver>().DefaultGrainDirectory`):
   * the pluggable CAS register/lookup/unregister surface, for tests that exercise
   * the directory contract directly rather than only through grain placement.
   */
  get directory(): GrainDirectory {
    return this.parts.node.grainDirectory();
  }

  /**
   * This silo's load-shedding test hooks (Orleans test-only
   * `ServiceProvider.GetRequiredService<TestHooksEnvironmentStatisticsProvider>()`/
   * `<OverloadDetector>()`): latch/unlatch its reported CPU usage or force it
   * overloaded outright, without going through a grain.
   */
  get loadShedding(): SiloLoadSheddingTestHooks {
    return this.parts.node.siloTestHooks();
  }

  /** The latest activation-rebalancer report, if rebalancing is enabled. */
  rebalancingReport(): RebalancingReport | undefined {
    return this.parts.rebalancerWorker?.report();
  }

  /**
   * The activation rebalancer's control surface (Orleans `IActivationRebalancer`,
   * `GetSiloServiceProvider().GetRequiredService<IActivationRebalancer>()` in the upstream
   * tests). Every operation is a no-op — `undefined`/does nothing — when rebalancing was never
   * enabled (`useActivationRebalancing`), the same as a silo with no `IActivationRebalancer`
   * registered.
   *
   * `force` is accepted for parity with Orleans' `GetRebalancingReport(bool force)` (which
   * forces a round-trip refresh from the elected worker through its monitor); this framework's
   * report is already live in-process, so it is accepted but has no effect.
   */
  getRebalancingReport(_force = false): RebalancingReport | undefined {
    return this.parts.rebalancerWorker?.report();
  }

  /** Suspend rebalancing (Orleans `SuspendRebalancing`). `durationMs` omitted suspends indefinitely; given, auto-resumes once it elapses. */
  suspendRebalancing(durationMs?: number): void {
    this.parts.rebalancerWorker?.suspend(durationMs);
  }

  /** Resume rebalancing if suspended, otherwise a no-op (Orleans `ResumeRebalancing`). */
  resumeRebalancing(): void {
    this.parts.rebalancerWorker?.resume();
  }

  /** Subscribe to rebalancer reports (Orleans `SubscribeToReports`/`IActivationRebalancerReportListener`). */
  subscribeToReports(listener: RebalancerReportListener): void {
    this.parts.rebalancerWorker?.subscribe(listener);
  }

  /** Unsubscribe a previously subscribed listener (Orleans `UnsubscribeFromReports`). */
  unsubscribeFromReports(listener: RebalancerReportListener): void {
    this.parts.rebalancerWorker?.unsubscribe(listener);
  }

  /**
   * The activation repartitioner's test/control surface (Orleans
   * `IActivationRepartitionerSystemTarget`, reached upstream via
   * `IActivationRepartitionerSystemTarget.GetReference(grainFactory, silo)`).
   * Every operation is a no-op — resolves/returns nothing meaningful — when
   * repartitioning was never enabled (`useActivationRepartitioning`).
   */
  async triggerRepartitionExchange(): Promise<void> {
    await this.parts.node.triggerRepartitionExchange();
  }

  resetRepartitionCounters(): void {
    this.parts.node.resetRepartitionCounters();
  }

  getRepartitionActivationCount(): number {
    return this.parts.node.getRepartitionActivationCount();
  }

  setRepartitionActivationCountOffset(offset: number): void {
    this.parts.node.setRepartitionActivationCountOffset(offset);
  }

  getRepartitionCallFrequencies(): ReadonlyArray<{ edge: Edge; count: number }> {
    return this.parts.node.getRepartitionCallFrequencies();
  }

  async flushRepartitionBuffers(): Promise<void> {
    await this.parts.node.flushRepartitionBuffers();
  }

  /** Run a sequence of optional hooks in order, awaiting each, with the given args. */
  private async runHooks<A extends unknown[]>(
    hooks: ReadonlyArray<(...args: A) => Promise<void>> | undefined,
    ...args: A
  ): Promise<void> {
    for (const hook of hooks ?? []) await hook(...args);
  }

  async start(): Promise<void> {
    const {
      node,
      health,
      healthServer,
      healthPort,
      membership,
      rebalancerWorker,
      selfProbeWorker,
      onStart,
      startupTasks,
    } = this.parts;
    // The membership watch's first list completes inside these (a
    // `KubernetesEndpointWatch.start()` awaits it), so the view is populated before
    // the node applies it.
    await this.runHooks(onStart);
    // Positioned now, before the node applies the current view: every view published
    // from here on is yielded once the watch below starts consuming, so none can fall
    // between the start-up reconcile and the watch.
    const updates = membership.updates();
    // Listens, then applies `membership.current()` — the ring, and join recovery for
    // the ranges it gains — so a startup task below can already call a grain.
    await node.start();
    // Finish that join recovery before the startup tasks and readiness. Every owned
    // operation waits for it anyway, so this only moves the wait ahead of them — and
    // keeps the node's recovery dials to members not listening yet from still being
    // in flight (and shared by the connection pool) when the first calls go out.
    await node.recoverySettled();
    await this.runHooks(startupTasks);
    health.update({ transportReady: true });
    if (healthServer !== undefined && healthPort !== undefined) {
      await healthServer.listen(healthPort);
    }
    // Ownership hooks for the applied view, then every later one (level-triggered),
    // whichever path applies it.
    this.stopViewListener = node.onViewApplied(() => this.reconcileView());
    this.reconcileView();
    await this.reconciling;
    rebalancerWorker?.start();
    selfProbeWorker?.start();
    this.watchMembership(updates);
    health.update({ membershipHealthy: node.hasAppliedView(), started: true });
  }

  async stop(): Promise<void> {
    const {
      rebalancerWorker,
      selfProbeWorker,
      reminderService,
      shutdown,
      healthServer,
      onStop,
      onBeforeDeactivate,
    } = this.parts;
    this.membershipWatch?.abort();
    this.stopViewListener?.();
    rebalancerWorker?.stop();
    reminderService?.stop();
    // Contained, unlike `onStop`: these run before the drain, so one failing
    // provider stop must not skip deactivating every activation, closing the
    // transport and the rest of the teardown (Orleans logs a lifecycle stage
    // that fails to stop and carries on stopping).
    const logger = this.parts.logger ?? noopLogger;
    for (const hook of onBeforeDeactivate ?? []) {
      try {
        await hook();
      } catch (error) {
        logger.error("pre-deactivation stop hook failed", { error });
      }
    }
    // `selfProbeWorker` keeps ticking through `shutdown.drain()`'s grace
    // period rather than stopping here alongside the other workers: it reads
    // `HealthCheck.isDraining()` to no-op its own flip once `drain()` sets
    // `draining` true, so a probe miss during the grace window (turns being
    // drained, connections closing) is correctly attributed to the drain
    // rather than misreported as a hung dispatcher. Only stopped once the
    // node itself is fully down.
    await shutdown.drain();
    selfProbeWorker?.stop();
    await healthServer?.close();
    await this.runHooks(onStop);
  }

  /**
   * React to membership view changes. Each published view only REQUESTS a
   * reconcile (`reconcileView`), which compares levels — the node's applied view
   * against `membership.current()` — so a view published while a reconcile is busy
   * in its hooks is picked up when they finish, never lost.
   */
  private watchMembership(updates: AsyncIterable<MembershipSnapshot>): void {
    const abort = new AbortController();
    this.membershipWatch = abort;
    void (async () => {
      for await (const _snapshot of updates) {
        if (abort.signal.aborted) return;
        this.reconcileView();
      }
    })();
  }

  /**
   * Bring this silo in line with the current membership view, level-triggered:
   * apply `membership.current()` to the node if it is behind, run the ownership
   * hooks (stream queues, durable-job shards, reminder ranges) for the view the node
   * has applied if they have not run for it yet, and go round again until neither
   * is behind. Intermediate views may be coalesced; the latest is always applied.
   *
   * Single-flight: a request while a reconcile runs (a view published mid-hook, or
   * the node applying a view on another path — the directory's stale-view refresh,
   * `awaitView` catching up to a peer — which `onViewApplied` reports) marks it to
   * go round again rather than starting a second, concurrent one.
   */
  private reconcileView(): void {
    if (this.reconciling !== undefined) {
      this.reconcileAgain = true;
      return;
    }
    const running = this.reconcileLoop().finally(() => {
      if (this.reconciling === running) this.reconciling = undefined;
    });
    this.reconciling = running;
  }

  private async reconcileLoop(): Promise<void> {
    // Yield once so the caller's `this.reconciling` is set before any synchronous
    // `updateView` below re-enters through `onViewApplied`.
    await Promise.resolve();
    const { node, health, membership, reminderService, onOwnershipChange } = this.parts;
    const logger = this.parts.logger ?? noopLogger;
    for (;;) {
      this.reconcileAgain = false;
      if (this.membershipWatch?.signal.aborted === true) return;
      let failed = false;
      try {
        if (node.appliedViewVersion !== membership.current().version) node.updateView();
        const version = node.appliedViewVersion;
        if (version !== this.hookedVersion) {
          // Recorded before the hooks run: a hook that fails is retried on the next
          // view change, not in a tight loop against a store that is down.
          this.hookedVersion = version;
          const ranges = node.ownedHashRanges();
          await this.runHooks(onOwnershipChange, ranges);
          await reminderService?.refreshOwnership(ranges);
        }
      } catch (error) {
        // Never let a failed view change END reconciliation (issue #70): a silo
        // frozen on the last view it managed to apply would never rebuild its ring
        // or re-adopt reminder ranges and stream queues again. `updateView()`
        // applies the view before any hook runs, so a failure here costs this
        // view's adoption work, which the next view change retries — and the hooks
        // do real I/O against Postgres/Redis/Kafka (durable-job shard claims,
        // pulling-stream queue ownership), so a store blip is the expected trigger.
        // `membershipHealthy` below follows the applied view, not the hooks: a
        // transient store error must not pull a serving silo out of the service.
        // (`LocalReminderService.refreshOwnership` already swallows its own store
        // errors and logs them.)
        failed = true;
        logger.error("membership view update failed", {
          version: membership.current().version,
          error,
        });
      }
      health.update({ membershipHealthy: node.hasAppliedView() });
      const behind =
        node.appliedViewVersion !== membership.current().version ||
        node.appliedViewVersion !== this.hookedVersion;
      if (!this.reconcileAgain && (!behind || failed)) return;
    }
  }
}

/** Build a `SiloHost` from configured parts. */
export function buildSiloHost(
  parts: Omit<SiloHostParts, "shutdown"> & { shutdown?: GracefulShutdown },
): SiloHost {
  const shutdown =
    parts.shutdown ??
    new GracefulShutdown(parts.health, parts.node, {
      ...(parts.gracefulShutdownMs !== undefined ? { graceMs: parts.gracefulShutdownMs } : {}),
      ...(parts.stopBudgetMs !== undefined ? { stopBudgetMs: parts.stopBudgetMs } : {}),
    });
  return new SiloHost({ ...parts, shutdown });
}
