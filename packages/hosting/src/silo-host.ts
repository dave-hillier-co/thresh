import type { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKeyFor } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import type { ClusterNode } from "@tsva/runtime/cluster-node";
import { GracefulShutdown } from "@tsva/hosting/graceful-shutdown";
import type { HealthCheck } from "@tsva/hosting/health-check";
import type { HealthServer } from "@tsva/hosting/health-server";

export interface SiloHostParts {
  node: ClusterNode;
  health: HealthCheck;
  healthServer: HealthServer | undefined;
  healthPort: number | undefined;
  shutdown: GracefulShutdown;
  membership: MembershipService;
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
    this.watchMembership();
    this.parts.health.update({
      membershipHealthy: this.parts.membership.current().silos.length > 0,
      started: true,
    });
  }

  async stop(): Promise<void> {
    this.membershipWatch?.abort();
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
