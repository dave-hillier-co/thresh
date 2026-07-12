import type { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import type { ClusterNode } from "@tsva/runtime/cluster-node";
import type { SiloLoadSheddingTestHooks } from "@tsva/runtime/load-shedding";
import type {
  ActivationRebalancerWorker,
  RebalancerReportListener,
} from "@tsva/runtime/placement/rebalancing/rebalancer-worker";
import type { RebalancingReport } from "@tsva/runtime/placement/rebalancing/rebalancing-report";
import { GracefulShutdown } from "@tsva/hosting/graceful-shutdown";
import type { HealthCheck } from "@tsva/hosting/health-check";
import type { HealthServer } from "@tsva/hosting/health-server";

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
  membership: MembershipService;
  reminderService?: ReminderService | undefined;
  /** The elected activation-rebalancer worker, started/stopped with the host. */
  rebalancerWorker?: ActivationRebalancerWorker | undefined;
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
   * Run after the node starts but before the silo is marked ready — e.g. call
   * grains from application startup code (Orleans `IStartupTask`).
   */
  startupTasks?: ReadonlyArray<() => Promise<void>>;
}

/**
 * A started silo: the cluster node plus its health probes and drain coordinator.
 * `start` brings it online and flips readiness; `stop` drains gracefully.
 */
export class SiloHost {
  private membershipWatch: AbortController | undefined;

  constructor(private readonly parts: SiloHostParts) {}

  get health(): HealthCheck {
    return this.parts.health;
  }

  /** The active ServiceId this silo is running with (Orleans test-hooks `GetServiceId()`). */
  get serviceId(): string {
    return this.parts.serviceId;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.parts.node.getGrain(def, key);
  }

  /** See `ClusterNode.getExtensionReference`: a reference to an explicit `GrainId` under `def`. */
  getExtensionReference<T>(def: GrainInterface<T>, grainId: GrainId): T {
    return this.parts.node.getExtensionReference(def, grainId);
  }

  isActive(id: GrainId): boolean {
    return this.parts.node.isActive(id);
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
      reminderService,
      rebalancerWorker,
      onStart,
      onOwnershipChange,
      startupTasks,
    } = this.parts;
    await this.runHooks(onStart);
    await node.start();
    await this.runHooks(startupTasks);
    health.update({ transportReady: true });
    if (healthServer !== undefined && healthPort !== undefined) {
      await healthServer.listen(healthPort);
    }
    const ranges = node.ownedHashRanges();
    await this.runHooks(onOwnershipChange, ranges);
    await reminderService?.refreshOwnership(ranges);
    rebalancerWorker?.start();
    this.watchMembership();
    health.update({
      membershipHealthy: membership.current().silos.length > 0,
      started: true,
    });
  }

  async stop(): Promise<void> {
    const { rebalancerWorker, reminderService, shutdown, healthServer, onStop } = this.parts;
    this.membershipWatch?.abort();
    rebalancerWorker?.stop();
    reminderService?.stop();
    await shutdown.drain();
    await healthServer?.close();
    await this.runHooks(onStop);
  }

  /** React to membership view changes: rebuild the ring and refresh health. */
  private watchMembership(): void {
    const { node, health, membership, reminderService, onOwnershipChange } = this.parts;
    const abort = new AbortController();
    this.membershipWatch = abort;
    void (async () => {
      for await (const snapshot of membership.updates()) {
        if (abort.signal.aborted) return;
        node.updateView();
        // Ring changed: take over (or release) reminder ranges and stream queues.
        const updated = node.ownedHashRanges();
        await this.runHooks(onOwnershipChange, updated);
        await reminderService?.refreshOwnership(updated);
        health.update({ membershipHealthy: snapshot.silos.length > 0 });
      }
    })();
  }
}

/** Build a `SiloHost` from configured parts. */
export function buildSiloHost(
  parts: Omit<SiloHostParts, "shutdown"> & { shutdown?: GracefulShutdown },
): SiloHost {
  const shutdown = parts.shutdown ?? new GracefulShutdown(parts.health, parts.node);
  return new SiloHost({ ...parts, shutdown });
}
