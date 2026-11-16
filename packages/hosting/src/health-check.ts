import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";

/** The conditions the silo reports to the Kubernetes probes (docs/05, docs/10). */
export interface HealthSignals {
  /** Startup complete: joined membership and accepting calls. */
  started: boolean;
  /** SIGTERM received; the silo is shutting down. */
  draining: boolean;
  /** Transport is listening and accepting connections. */
  transportReady: boolean;
  /**
   * The membership source has completed a list and this silo is listed in the view, whatever
   * its status. It does not ask for a ring: a joining silo owns nothing while a peer is active,
   * and must still report ready for Kubernetes to mark it ready.
   */
  membershipHealthy: boolean;
  /**
   * The silo has fenced itself: a view omitted it after it had been listed, so its peers have
   * dropped its directory entries (Orleans `MembershipTableManager.KillMyselfLocally`). Fails
   * liveness as well as readiness, so the kubelet restarts the pod; a fenced silo never serves
   * again in this incarnation.
   */
  fenced: boolean;
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
  /**
   * When the self-probe's current unbroken run of failures began (the first miss, not the
   * readiness flip at `missedThreshold`), or `undefined` while the probe is succeeding. Liveness
   * reads it against `HealthCheckOptions.livenessFailureMs`.
   */
  probeFailingSinceMs: number | undefined;
}

export interface HealthCheckOptions {
  /** The clock liveness measures the failure period on (defaults to the system clock). */
  time?: TimeProvider;
  /**
   * `live()` fails once the self-probe has failed continuously for this long, so the kubelet
   * restarts a silo whose dispatcher is permanently wedged (readiness alone only pulls it from
   * service; nothing would ever restart it). Unset, liveness never depends on the probe: a
   * restart discards every activation and re-runs recovery, so it is an operator's decision, and
   * the period must comfortably exceed the time a slow-but-recovering silo needs.
   */
  livenessFailureMs?: number;
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
 *   is the signal; a wedged process simply won't respond and is restarted) and, only when
 *   `livenessFailureMs` is configured, the self-probe has not failed continuously for that long.
 *   It never depends on the Kubernetes API: an outage there freezes the membership view and
 *   must neither pull silos from service nor restart them. It fails once the silo has fenced
 *   itself (`fenced`), which only a view that omits this silo after listing it causes.
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
    fenced: false,
    overloaded: false,
    dispatcherResponsive: true,
    probeFailingSinceMs: undefined,
  };
  private overloadSource: (() => boolean) | undefined;
  private readonly time: TimeProvider;
  private readonly livenessFailureMs: number | undefined;

  constructor(options: HealthCheckOptions = {}) {
    this.time = options.time ?? systemTimeProvider;
    this.livenessFailureMs = options.livenessFailureMs;
  }

  /**
   * Take `overloaded` from a live source (the node's load-shedding detector) instead of a flag
   * someone must remember to push: readiness reads it at probe time, so it follows the detector
   * in both directions. A `true` from `update({ overloaded })` still counts.
   */
  followOverload(source: () => boolean): void {
    this.overloadSource = source;
  }

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
    const since = this.signals.probeFailingSinceMs;
    const dispatcherResponsive =
      this.livenessFailureMs === undefined ||
      since === undefined ||
      this.signals.draining ||
      this.time.now() - since < this.livenessFailureMs;
    const notFenced = !this.signals.fenced;
    return {
      ok: dispatcherResponsive && notFenced,
      checks: { process: true, dispatcherResponsive, notFenced },
    };
  }

  startup(): ProbeResult {
    return { ok: this.signals.started, checks: { started: this.signals.started } };
  }

  ready(): ProbeResult {
    const checks = {
      started: this.signals.started,
      transportReady: this.signals.transportReady,
      membershipHealthy: this.signals.membershipHealthy && !this.signals.fenced,
      notDraining: !this.signals.draining,
      notOverloaded: !(this.signals.overloaded || this.overloadSource?.() === true),
      dispatcherResponsive: this.signals.dispatcherResponsive,
    };
    return { ok: Object.values(checks).every(Boolean), checks };
  }
}
