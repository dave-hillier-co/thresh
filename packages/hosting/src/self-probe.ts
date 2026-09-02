/**
 * Self-probe liveness worker — docs/design-notes-parity-gaps.md item 9,
 * option A. K8s' readiness probe alone can't see a silo whose HTTP endpoint
 * still answers 200 but whose grain dispatcher has deadlocked (a wedged turn
 * scheduler, an exhausted admission queue): the endpoint would keep serving
 * traffic into a silo that can no longer complete a single call. This worker
 * closes that gap the way Orleans' probe graph does for a PEER silo, but
 * turned inward — no gossip, no cluster-wide agreement, K8s stays the sole
 * membership authority (see the "Constraint" note in the design doc).
 *
 * On a timer, it calls a no-op system grain (`ISiloProbe`, `preferLocal`
 * placement) on THIS silo and tracks consecutive misses. Reaching
 * `missedThreshold` flips `HealthCheck.dispatcherResponsive` false, which
 * pulls the endpoint from service exactly the way `draining` does; a
 * subsequent success flips it back. The cadence defaults
 * (`intervalMs`/`timeoutMs`/`missedThreshold`) are the same ballpark as
 * Orleans' `ClusterMembershipOptions.ProbeTimeout` (10s) and
 * `NumMissedProbesLimit` (3) — a wedged silo is declared dead after roughly
 * the same elapsed time here as it would be by an Orleans probe-graph peer.
 *
 * Modelled on `ActivationRebalancerWorker`: self-reschedules off
 * `TimeProvider.setTimer`, so the whole loop is deterministic under a fake
 * clock in tests, and a failed tick never kills the loop.
 */
import type { HealthCheck } from "@thresh/hosting/health-check";
import type { TimeProvider, TimerHandle } from "@thresh/core/time-provider";
import { withCallOptions } from "@thresh/runtime/invocation-context";

/** Orleans `ClusterMembershipOptions` defaults this mirrors for the self-probe cadence. */
const DEFAULT_INTERVAL_MS = 10_000;
const DEFAULT_TIMEOUT_MS = 5_000;
const DEFAULT_MISSED_THRESHOLD = 3;

export interface SelfProbeWorkerOptions {
  /** Calls the no-op `ISiloProbe` grain on this silo — `() => node.getGrain(ISiloProbe, local.ringKey).ping()`. */
  probe: () => Promise<void>;
  /** Flipped on `missedThreshold` consecutive misses / recovered on the next success. */
  health: HealthCheck;
  /** The shared clock (inject a fake in tests). */
  time: TimeProvider;
  /** How often to probe. Defaults to 10s (Orleans `ProbeTimeout`). */
  intervalMs?: number;
  /** How long one probe may take before it counts as a miss. Defaults to 5s. */
  timeoutMs?: number;
  /** Consecutive misses before readiness flips false. Defaults to 3 (Orleans `NumMissedProbesLimit`). */
  missedThreshold?: number;
}

/**
 * The timer-driven self-probe worker. `start`/`stop` mirror
 * `ActivationRebalancerWorker`'s lifecycle exactly, so the silo builder wires
 * it in the same shape as every other clock-driven worker.
 */
export class SelfProbeWorker {
  private handle: TimerHandle | undefined;
  private stopped = true;
  private consecutiveMisses = 0;
  private readonly intervalMs: number;
  private readonly timeoutMs: number;
  private readonly missedThreshold: number;

  constructor(private readonly options: SelfProbeWorkerOptions) {
    this.intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.missedThreshold = options.missedThreshold ?? DEFAULT_MISSED_THRESHOLD;
  }

  /** Begin probing, one interval from now. */
  start(): void {
    this.stopped = false;
    this.scheduleNext();
  }

  /** Stop probing and cancel any pending tick. */
  stop(): void {
    this.stopped = true;
    if (this.handle !== undefined) {
      this.options.time.clearTimer(this.handle);
      this.handle = undefined;
    }
  }

  private scheduleNext(): void {
    this.handle = this.options.time.setTimer(() => {
      if (this.stopped) return;
      void this.tick();
    }, this.intervalMs);
  }

  private async tick(): Promise<void> {
    try {
      await this.probeOnce();
      this.consecutiveMisses = 0;
      // A recovered silo re-enters service — but never flip a signal a drain
      // already owns: a probe that misses (or, here, succeeds) only because
      // the silo is mid-shutdown says nothing about the dispatcher's health.
      if (!this.options.health.isDraining()) {
        this.options.health.update({ dispatcherResponsive: true });
      }
    } catch {
      this.consecutiveMisses += 1;
      if (this.consecutiveMisses >= this.missedThreshold && !this.options.health.isDraining()) {
        this.options.health.update({ dispatcherResponsive: false });
      }
    } finally {
      if (!this.stopped) this.scheduleNext();
    }
  }

  /**
   * One probe call, deadline-enforced. `withCallOptions` stamps a REAL
   * per-call deadline on the outgoing request (`@thresh/runtime`'s shipped
   * deadline API), so this exercises the genuine admission-time deadline
   * check the same way any other deadlined call would.
   *
   * That alone is not enough to guarantee THIS worker notices a hang, though:
   * `Turn.signal` is checked only at admission (turn-scheduler.ts) — once a
   * turn is running, or while queued behind one that never finishes (exactly
   * the deadlocked-scheduler failure this feature exists to catch), the
   * deadline firing has no effect and the call would simply never settle. So
   * this also races the call against its own timer, the same pattern
   * `GrainFactory.raceResponseDeadline` uses for a call's response timeout:
   * on timeout it rejects and swallows the call's eventual settlement so a
   * late resolve/reject after the race is already decided never surfaces as
   * an unhandled rejection.
   */
  private probeOnce(): Promise<void> {
    const call = withCallOptions({ deadlineMs: this.timeoutMs }, () => this.options.probe());
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const timer = this.options.time.setTimer(() => {
        if (settled) return;
        settled = true;
        reject(new Error(`self-probe exceeded its ${this.timeoutMs}ms deadline`));
        call.catch(() => {});
      }, this.timeoutMs);
      call.then(
        () => {
          if (settled) return;
          settled = true;
          this.options.time.clearTimer(timer);
          resolve();
        },
        (error: unknown) => {
          if (settled) return;
          settled = true;
          this.options.time.clearTimer(timer);
          reject(error instanceof Error ? error : new Error(String(error)));
        },
      );
    });
  }
}
