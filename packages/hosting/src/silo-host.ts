import type { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import type { ClusterNode } from "@tsva/runtime/cluster-node";
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
  health: HealthCheck;
  healthServer: HealthServer | undefined;
  healthPort: number | undefined;
  shutdown: GracefulShutdown;
  membership: MembershipService;
  reminderService?: ReminderService | undefined;
}

// Single-silo hosting owns the whole ring; multi-silo ownership from the ring is a refinement.
const WHOLE_RING: readonly [number, number] = [0, 0x1_0000_0000];

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

  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.parts.node.getGrain(def, key);
  }

  isActive(id: GrainId): boolean {
    return this.parts.node.isActive(id);
  }

  async start(): Promise<void> {
    await this.parts.node.start();
    this.parts.health.update({ transportReady: true });
    if (this.parts.healthServer !== undefined && this.parts.healthPort !== undefined) {
      await this.parts.healthServer.listen(this.parts.healthPort);
    }
    await this.parts.reminderService?.refreshOwnership([WHOLE_RING]);
    this.watchMembership();
    this.parts.health.update({
      membershipHealthy: this.parts.membership.current().silos.length > 0,
      started: true,
    });
  }

  async stop(): Promise<void> {
    this.membershipWatch?.abort();
    this.parts.reminderService?.stop();
    await this.parts.shutdown.drain();
    await this.parts.healthServer?.close();
  }

  /** React to membership view changes: rebuild the ring and refresh health. */
  private watchMembership(): void {
    const abort = new AbortController();
    this.membershipWatch = abort;
    void (async () => {
      for await (const snapshot of this.parts.membership.updates()) {
        if (abort.signal.aborted) return;
        this.parts.node.updateView();
        this.parts.health.update({ membershipHealthy: snapshot.silos.length > 0 });
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
