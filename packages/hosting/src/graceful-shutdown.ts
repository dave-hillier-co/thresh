import type { HealthCheck } from "@thresh/hosting/health-check";

/**
 * Something that can be drained and stopped (a silo / cluster node).
 * `deadlineMs`, when given, bounds how long `stop()` waits out its own
 * internal work (e.g. `ClusterNode.stop()` deactivating every activation) —
 * see `GracefulShutdownOptions.stopBudgetMs`.
 */
export interface Drainable {
  stop(deadlineMs?: number): Promise<void>;
}

export interface GracefulShutdownOptions {
  /** Time to let readiness propagate and in-flight turns finish before stopping. Defaults to `DEFAULT_GRACE_MS`. */
  graceMs?: number;
  /** Injectable delay so the grace period is deterministic in tests. */
  delay?: (ms: number) => Promise<void>;
  /**
   * Overall budget for the WHOLE drain — `graceMs` plus `node.stop()` —
   * mirroring Orleans cancelling `DeactivateAllActivations` with the host's
   * own stop token rather than letting it (and everything queued behind a
   * slow `onDeactivate` hook) run unbounded. Without this, `graceMs` plus
   * however long deactivation takes can exceed the process's own termination
   * grace period and the silo gets SIGKILLed mid-stop instead of finishing
   * cleanly (issue #108). `node.stop()` gets whatever's left of the budget
   * after `graceMs`, floored at 0. Defaults to `DEFAULT_STOP_BUDGET_MS`.
   */
  stopBudgetMs?: number;
}

/**
 * Default grace period between flipping readiness to not-ready and actually stopping the node.
 * A Kubernetes Service's EndpointSlice controller needs a moment to notice the readiness flip and
 * remove this pod's endpoint, and every peer's own membership/readiness probe polls on an
 * interval rather than reacting instantly — so a grace of 0 (the old default) let `stop()` tear
 * the node down before any of that propagated, and a peer (or the Service) could still route a
 * request here after this silo considered itself gone. 5s covers typical EndpointSlice
 * propagation plus at least one probe interval with room to spare; still overridable via
 * `GracefulShutdownOptions.graceMs`.
 */
export const DEFAULT_GRACE_MS = 5000;

/**
 * Default overall stop budget (`GracefulShutdownOptions.stopBudgetMs`) — the
 * same 30s Orleans' own host defaults its shutdown timeout to
 * (`HostOptions.ShutdownTimeout`, e.g. `TestClusterHostFactory.cs:48`), and
 * the same magnitude this codebase already caps a single activation's
 * `onDeactivate` hook at by default (`DEFAULT_DEACTIVATION_TIMEOUT_MS`,
 * `silo-builder.ts`).
 */
export const DEFAULT_STOP_BUDGET_MS = 30_000;

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
    const graceMs = this.options.graceMs ?? DEFAULT_GRACE_MS;
    await delay(graceMs);
    const budgetMs = this.options.stopBudgetMs ?? DEFAULT_STOP_BUDGET_MS;
    await this.node.stop(Math.max(0, budgetMs - graceMs));
  }

  // A `SIGTERM`-installing helper deliberately does NOT live here: this
  // class only drains the `Drainable` it was built with (`ClusterNode`, at
  // `buildSiloHost`) — `SiloHost.stop()` runs the REST of a full teardown
  // (reminder service, self-probe worker, health server, `onStop` hooks)
  // afterward, and this class has no reference to that. A `.install()` used
  // to wire a SIGTERM handler straight to `drain()`, calling only
  // `node.stop()` and skipping all of that — silently incomplete, and
  // nothing in this codebase ever called it (every real entry point installs
  // its own handler against `SiloHost.stop()`, e.g.
  // `examples/k8s-silo/src/main.ts`). Removed rather than fixed in place:
  // this class has no seam to reach a full teardown sequence it doesn't own.
}

function defaultDelay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
