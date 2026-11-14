import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import type { KeyTypeOf } from "@thresh/core/key-kinds";
import { type Logger, defaultLogger, safeLogger } from "@thresh/core/logger";
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
import { LifecycleSubject, ServiceLifecycleStage } from "@thresh/runtime/lifecycle-subject";
import { GracefulShutdown } from "@thresh/hosting/graceful-shutdown";
import type { HealthCheck } from "@thresh/hosting/health-check";
import type { HealthServer } from "@thresh/hosting/health-server";
import type { SelfProbeWorker } from "@thresh/hosting/self-probe";

/** The reminder service the host drives (a `LocalReminderService`). */
export interface ReminderService {
  refreshOwnership(ranges: ReadonlyArray<readonly [number, number]>): Promise<void>;
  stop(): void;
}

/**
 * One subsystem's place in the silo lifecycle (Orleans `ILifecycleParticipant`): `start` runs when
 * the host reaches `stage` on the way up, `stop` when it passes it on the way down.
 */
export interface SiloLifecycleParticipant {
  readonly name: string;
  readonly stage: number;
  readonly start?: () => Promise<void>;
  readonly stop?: () => Promise<void>;
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
  /**
   * Every other subsystem's start and stop, each at a named stage (see
   * `ServiceLifecycleStage`). The host adds its own — the node, startup tasks, the health
   * listener, the view reconcile, the rebalancer, the self-probe, reminders and readiness — to the
   * same lifecycle, starts the stages ascending and stops them descending, and rolls back a start
   * that fails.
   */
  lifecycle?: ReadonlyArray<SiloLifecycleParticipant>;
  /**
   * Run on start and on every membership change with this silo's owned hash
   * ranges — e.g. take over the stream queues the ring now assigns this silo.
   */
  onOwnershipChange?: ReadonlyArray<
    (ranges: ReadonlyArray<readonly [number, number]>) => Promise<void>
  >;
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
  private lifecycle: LifecycleSubject | undefined;
  private stopping: Promise<void> | undefined;
  /** Set once readiness is published; a stop after that waits the drain grace. */
  private serving = false;
  /** The membership feed, positioned before the node applies the first view. */
  private updates: AsyncIterable<MembershipSnapshot> | undefined;
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

  /**
   * The silo's lifecycle, one subject per start. The host's own subsystems sit at these stages
   * (Orleans' names and order), with the builder's participants around them:
   *
   * - `RuntimeInitialize`: storage clients, the membership watch, the health listener's close.
   *   Below the node, so they come up before it listens and are torn down after it has
   *   deactivated every activation.
   * - `RuntimeServices`: the transport and node. Start applies the membership view, then settles
   *   join recovery. Stop waits out the grace period and deactivates under the stop budget.
   * - `RuntimeStorageServices`, `RuntimeGrainServices`: stream providers, grain services.
   * - `ApplicationServices`: startup tasks.
   * - `BecomeActive`: the health listener opens; pulling agents and the job manager stop here,
   *   before activations are deactivated.
   * - `Active`: the membership reconcile (ownership hooks), the rebalancer, the self-probe and
   *   reminders.
   * - `Last`: readiness. It is published last on the way up and flipped first on the way down.
   */
  private buildLifecycle(): LifecycleSubject {
    const {
      node,
      health,
      healthServer,
      healthPort,
      membership,
      rebalancerWorker,
      selfProbeWorker,
    } = this.parts;
    const { reminderService, shutdown, startupTasks } = this.parts;
    const logger = safeLogger(this.parts.logger ?? defaultLogger);
    const subject = new LifecycleSubject({
      onError: ({ phase, observerName, error }) => {
        logger.error(phase === "stop" ? "silo stop hook failed" : "silo start hook failed", {
          phase: observerName,
          error,
        });
      },
    });
    const subscribe = (p: SiloLifecycleParticipant): void => {
      subject.subscribe(p.name, p.stage, {
        onStart: async () => p.start?.(),
        onStop: async () => p.stop?.(),
      });
    };
    for (const participant of this.parts.lifecycle ?? []) subscribe(participant);

    // The membership watch's first list completes inside a `RuntimeInitialize` start (a
    // `KubernetesEndpointWatch.start()` awaits it), so the view is populated before the node
    // applies it.
    subscribe({
      name: "node",
      stage: ServiceLifecycleStage.RuntimeServices,
      start: async () => {
        // Positioned now, before the node applies the current view: every view published from
        // here on is yielded once the watch starts consuming, so none can fall between the
        // start-up reconcile and the watch.
        this.updates = membership.updates();
        // Listens, then applies `membership.current()` — the ring, and join recovery for the
        // ranges it gains — so a startup task can already call a grain.
        await node.start();
        // Finish that join recovery before the startup tasks and readiness. Every owned
        // operation waits for it anyway, so this only moves the wait ahead of them — and keeps
        // the node's recovery dials to members not listening yet from still being in flight (and
        // shared by the connection pool) when the first calls go out.
        await node.recoverySettled();
      },
      // A silo that was serving waits out the grace period; one rolling back a failed start has
      // nothing to wait for.
      stop: async () => (this.serving ? shutdown.drain() : shutdown.stopWithoutGrace()),
    });
    subscribe({
      name: "startupTasks",
      stage: ServiceLifecycleStage.ApplicationServices,
      start: async () => {
        for (const task of startupTasks ?? []) await task();
      },
    });
    subscribe({
      name: "health listener",
      stage: ServiceLifecycleStage.BecomeActive,
      start: async () => {
        health.update({ transportReady: true });
        // Readiness follows the listener, not the moment it was last seen up.
        node.onListenerClosed(() => health.update({ transportReady: false }));
        if (healthServer !== undefined && healthPort !== undefined) {
          await healthServer.listen(healthPort);
        }
      },
    });
    // Closed after the node stops, so the readiness endpoint keeps answering through the drain.
    subscribe({
      name: "health server",
      stage: ServiceLifecycleStage.RuntimeInitialize,
      stop: async () => healthServer?.close(),
    });
    subscribe({
      name: "membership reconcile",
      stage: ServiceLifecycleStage.Active,
      start: async () => {
        const watch = new AbortController();
        this.membershipWatch = watch;
        this.hookedVersion = undefined;
        // Ownership hooks for the applied view, then every later one (level-triggered),
        // whichever path applies it.
        this.stopViewListener = node.onViewApplied(() => this.reconcileView());
        this.reconcileView();
        await this.reconciling;
        if (this.updates !== undefined) this.watchMembership(this.updates, watch);
      },
      stop: async () => {
        this.membershipWatch?.abort();
        this.stopViewListener?.();
        this.stopViewListener = undefined;
      },
    });
    subscribe({
      name: "rebalancer",
      stage: ServiceLifecycleStage.Active,
      start: async () => rebalancerWorker?.start(),
      stop: async () => rebalancerWorker?.stop(),
    });
    subscribe({
      name: "self-probe",
      stage: ServiceLifecycleStage.Active,
      start: async () => selfProbeWorker?.start(),
      stop: async () => selfProbeWorker?.stop(),
    });
    subscribe({
      name: "reminders",
      stage: ServiceLifecycleStage.Active,
      stop: async () => reminderService?.stop(),
    });
    subscribe({
      name: "readiness",
      stage: ServiceLifecycleStage.Last,
      start: async () => {
        health.update({ membershipHealthy: node.hasAppliedView(), started: true });
        this.serving = true;
      },
      stop: async () => shutdown.beginDrain(),
    });
    return subject;
  }

  /**
   * Start every stage in ascending order. A start that fails stops the stages already started, in
   * reverse (the failed stage included, as Orleans' `LifecycleSubject` does), and rejects with the
   * original error, so nothing is left listening, connected or armed.
   */
  async start(): Promise<void> {
    const subject = this.buildLifecycle();
    this.lifecycle = subject;
    this.stopping = undefined;
    this.serving = false;
    try {
      await subject.onStart();
    } catch (error) {
      await subject.onStop();
      throw error;
    }
  }

  /**
   * Stop every stage in descending order, readiness first. A participant whose stop fails is
   * logged and the stages below it still stop. Idempotent.
   */
  stop(): Promise<void> {
    this.stopping ??= this.stopLifecycle();
    return this.stopping;
  }

  private async stopLifecycle(): Promise<void> {
    if (this.lifecycle === undefined) {
      this.parts.shutdown.beginDrain();
      return;
    }
    await this.lifecycle.onStop();
  }

  /**
   * React to membership view changes. Each published view only REQUESTS a
   * reconcile (`reconcileView`), which compares levels — the node's applied view
   * against `membership.current()` — so a view published while a reconcile is busy
   * in its hooks is picked up when they finish, never lost.
   */
  private watchMembership(
    updates: AsyncIterable<MembershipSnapshot>,
    abort: AbortController,
  ): void {
    void (async () => {
      try {
        for await (const _snapshot of updates) {
          if (abort.signal.aborted) return;
          this.reconcileView();
        }
      } catch (error) {
        // The feed itself failed: nothing is left to drive reconciliation, which is exactly the
        // frozen-view failure that must not be silent.
        safeLogger(this.parts.logger ?? defaultLogger).error(
          "membership watch ended with an error",
          {
            error,
          },
        );
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
    const logger = safeLogger(this.parts.logger ?? defaultLogger);
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
