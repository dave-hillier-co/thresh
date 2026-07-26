import type { HealthCheck } from "@thresh/hosting/health-check";

/** Something that can be drained and stopped (a silo / cluster node). */
export interface Drainable {
  stop(): Promise<void>;
}

export interface GracefulShutdownOptions {
  /** Time to let readiness propagate and in-flight turns finish before stopping. */
  graceMs?: number;
  /** Injectable delay so the grace period is deterministic in tests. */
  delay?: (ms: number) => Promise<void>;
}

/**
 * Drains a silo on shutdown (docs/03, docs/05): flip readiness to not-ready
 * first so Kubernetes pulls the pod from the service endpoints, wait out the
 * grace period for in-flight turns, then stop the node (deactivating grains and
 * unregistering directory entries). Idempotent.
 */
export class GracefulShutdown {
  private draining = false;

  constructor(
    private readonly health: HealthCheck,
    private readonly node: Drainable,
    private readonly options: GracefulShutdownOptions = {},
  ) {}

  async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    this.health.update({ draining: true });
    const delay = this.options.delay ?? defaultDelay;
    await delay(this.options.graceMs ?? 0);
    await this.node.stop();
  }

  /** Install a SIGTERM handler that drains; returns an uninstall function. */
  install(): () => void {
    const handler = () => void this.drain();
    process.on("SIGTERM", handler);
    return () => {
      process.off("SIGTERM", handler);
    };
  }
}

function defaultDelay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
