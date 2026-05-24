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
 *   not overloaded. Flipping to not-ready on drain pulls the silo from the
 *   service endpoints before it stops.
 */
export class HealthCheck {
  private signals: HealthSignals = {
    started: false,
    draining: false,
    transportReady: false,
    membershipHealthy: false,
    overloaded: false,
  };

  update(partial: Partial<HealthSignals>): void {
    this.signals = { ...this.signals, ...partial };
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
    };
    return { ok: Object.values(checks).every(Boolean), checks };
  }
}
