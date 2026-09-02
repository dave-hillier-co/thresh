/** The conditions the silo reports to the Kubernetes probes (docs/05, docs/10). */
export interface HealthSignals {
  /** Startup complete: joined membership and accepting calls. */
  started: boolean;
  /** SIGTERM received; the silo is shutting down. */
  draining: boolean;
  /** Transport is listening and accepting connections. */
  transportReady: boolean;
  /** The membership watch is connected and current. */
  membershipHealthy: boolean;
  /** The silo is over capacity and should shed new placements. */
  overloaded: boolean;
  /**
   * The grain dispatcher answered its own self-probe within the last few
   * probe cycles (docs/design-notes-parity-gaps.md item 9, option A). A silo
   * can return 200 from every OTHER check here and still have a hung
   * dispatcher (deadlocked turn scheduler, exhausted queue) that k8s'
   * readiness probe alone can't see — `SelfProbeWorker` flips this false
   * after `missedThreshold` consecutive timeouts calling a no-op system grain
   * on itself, so `ready()` below pulls the endpoint from service the same
   * way `draining` does. Starts `true` (optimistic) so a fresh silo isn't
   * held not-ready before the first probe cycle has even run.
   */
  dispatcherResponsive: boolean;
}

export interface ProbeResult {
  ok: boolean;
  checks: Record<string, boolean>;
}

/**
 * Computes the liveness, readiness and startup probe results from the silo's
 * current health signals.
 *
 * - **live**: the process is up and the event loop responsive (answering at all
 *   is the signal; a wedged process simply won't respond and is restarted).
 * - **startup**: the silo has finished starting.
 * - **ready**: started, transport accepting, membership healthy, not draining,
 *   not overloaded, dispatcher responsive to its own self-probe. Flipping to
 *   not-ready on drain pulls the silo from the service endpoints before it
 *   stops; flipping on a hung dispatcher does the same for a silo that is
 *   technically up but can no longer serve calls.
 */
export class HealthCheck {
  private signals: HealthSignals = {
    started: false,
    draining: false,
    transportReady: false,
    membershipHealthy: false,
    overloaded: false,
    dispatcherResponsive: true,
  };

  update(partial: Partial<HealthSignals>): void {
    this.signals = { ...this.signals, ...partial };
  }

  /**
   * Whether the silo is currently draining — the minimal surface
   * `SelfProbeWorker` needs to gate its readiness flip: a probe that misses
   * because the silo is shutting down (turns being drained, connections
   * closing) must not be mistaken for a hung dispatcher, since `draining`
   * already pulls the endpoint from service on its own.
   */
  isDraining(): boolean {
    return this.signals.draining;
  }

  live(): ProbeResult {
    return { ok: true, checks: { process: true } };
  }

  startup(): ProbeResult {
    return { ok: this.signals.started, checks: { started: this.signals.started } };
  }

  ready(): ProbeResult {
    const checks = {
      started: this.signals.started,
      transportReady: this.signals.transportReady,
      membershipHealthy: this.signals.membershipHealthy,
      notDraining: !this.signals.draining,
      notOverloaded: !this.signals.overloaded,
      dispatcherResponsive: this.signals.dispatcherResponsive,
    };
    return { ok: Object.values(checks).every(Boolean), checks };
  }
}
