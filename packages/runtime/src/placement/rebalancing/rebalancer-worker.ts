/**
 * The activation rebalancer's elected singleton worker (ADR 0016, slice 2b) — a
 * faithful port of Orleans'
 * `Orleans.Runtime/Placement/Rebalancing/ActivationRebalancerWorker.cs`. Orleans
 * runs it as a `[KeepAlive, Immovable]` grain on a single, deterministically
 * elected silo; here we elect a leader from the membership view and only act on
 * that silo, so exactly one node drives the cluster-wide cycle at a time.
 *
 * On each timer tick the elected silo runs one rebalancing cycle (which gathers
 * load, plans migrations from the entropy model, and executes them), threads the
 * model's `CycleState` between ticks, and on a `stop` reason backs off: a
 * completed session cools down for one cycle period and resets to the initial
 * state; a stagnated session backs off exponentially (Orleans' `failedSessions`).
 *
 * The clock is the only injected boundary (a `TimeProvider`), so the loop is
 * deterministic under a fake clock; the cycle runner and membership are injected
 * so this stays off `ClusterNode` (which already exposes `runRebalanceCycle`).
 */
import type { MembershipService } from "@tsva/core/membership";
import { activeSilos } from "@tsva/core/membership";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { TimeProvider, TimerHandle } from "@tsva/core/time-provider";
import type {
  CycleState,
  MigrationMove,
  RebalancerOptions,
  StopReason,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import { INITIAL_CYCLE_STATE } from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import type {
  RebalancerStatus,
  RebalancingReport,
} from "@tsva/runtime/placement/rebalancing/rebalancing-report";

/**
 * The outcome of running one cycle, as `ClusterNode.runRebalanceCycle` returns
 * it. `moves` is optional (the node does not currently surface it) and, when
 * present, lets the worker attribute per-silo dispersed/acquired counts in its
 * report.
 */
export interface RebalanceCycleResult {
  nextState: CycleState;
  imbalance: number;
  moved: number;
  stop?: StopReason;
  moves?: readonly MigrationMove[];
}

/** Runs one rebalancing cycle from the given state (e.g. `ClusterNode.runRebalanceCycle`). */
export type RunRebalanceCycle = (state: CycleState) => Promise<RebalanceCycleResult>;

export interface ActivationRebalancerWorkerOptions {
  /** This silo's address (the election candidate). */
  local: SiloAddress;
  /** The live silo set; the leader is the lowest active ring key. */
  membership: MembershipService;
  /** The shared clock (inject a fake in tests). */
  time: TimeProvider;
  /** Runs one cycle — gather load, plan, migrate; threads cycle state. */
  runCycle: RunRebalanceCycle;
  /** The entropy-model tunables (carried so the report and backoff agree with the model). */
  options: RebalancerOptions;
  /** How long between cycles within a session. */
  sessionCyclePeriodMs: number;
}

const EMPTY_REPORT_COUNTS: ReadonlyMap<string, number> = new Map();

/**
 * The elected, timer-driven rebalancer worker. Self-reschedules off
 * `time.setTimer` (modelled on `ActivationCollector`), keeps the model's
 * `CycleState` in memory, and exposes `start`/`stop` plus `suspend`/`resume`.
 */
export class ActivationRebalancerWorker {
  private handle: TimerHandle | undefined;
  private stopped = true;
  private suspended = false;
  private state: CycleState = INITIAL_CYCLE_STATE;
  /** Consecutive stagnated sessions, driving exponential backoff (Orleans `failedSessions`). */
  private failedSessions = 0;
  private status: RebalancerStatus = "executing";
  private latestImbalance = 0;
  private dispersed: ReadonlyMap<string, number> = EMPTY_REPORT_COUNTS;
  private acquired: ReadonlyMap<string, number> = EMPTY_REPORT_COUNTS;
  /** Guards against overlapping ticks if a cycle outruns its period. */
  private running = false;

  constructor(private readonly options: ActivationRebalancerWorkerOptions) {}

  /** Begin the cycle loop, scheduling the first tick one period out. */
  start(): void {
    this.stopped = false;
    this.scheduleNext(this.options.sessionCyclePeriodMs);
  }

  /** Stop the loop and cancel any pending tick. */
  stop(): void {
    this.stopped = true;
    if (this.handle !== undefined) {
      this.options.time.clearTimer(this.handle);
      this.handle = undefined;
    }
  }

  /** Pause cycle execution without tearing down the loop (Orleans `SuspendRebalancing`). */
  suspend(): void {
    this.suspended = true;
    this.status = "suspended";
  }

  /** Resume cycle execution after a suspend. */
  resume(): void {
    this.suspended = false;
    this.status = "executing";
  }

  /** The latest observable status (Orleans `RebalancingReport`). */
  report(): RebalancingReport {
    return {
      host: this.options.local.ringKey,
      status: this.status,
      clusterImbalance: this.latestImbalance,
      dispersed: this.dispersed,
      acquired: this.acquired,
    };
  }

  /** The deterministically elected leader: the lowest active ring key. */
  private leader(): SiloAddress | undefined {
    const active = activeSilos(this.options.membership.current());
    if (active.length === 0) return undefined;
    return active.reduce((lowest, silo) => (silo.ringKey < lowest.ringKey ? silo : lowest));
  }

  private isLeader(): boolean {
    const leader = this.leader();
    return leader !== undefined && leader.equals(this.options.local);
  }

  private scheduleNext(delayMs: number): void {
    this.handle = this.options.time.setTimer(() => {
      if (this.stopped) return;
      void this.tick();
    }, delayMs);
  }

  private async tick(): Promise<void> {
    // Default: re-arm one period out unless a stop reason overrides the delay.
    let nextDelay = this.options.sessionCyclePeriodMs;
    try {
      if (this.suspended || this.running || !this.isLeader()) return;
      this.running = true;
      const result = await this.options.runCycle(this.state);
      this.latestImbalance = result.imbalance;
      this.updateReportCounts(result.moves);

      if (result.stop === "complete") {
        // Session balanced: reset and cool down one cycle period before the next
        // session — which is exactly the loop's normal cadence (ADR 0016).
        this.state = INITIAL_CYCLE_STATE;
        this.failedSessions = 0;
        nextDelay = this.options.sessionCyclePeriodMs;
      } else if (result.stop === "stagnated") {
        // Session gave up: reset and back off exponentially on consecutive failures.
        this.state = INITIAL_CYCLE_STATE;
        this.failedSessions += 1;
        const factor = 2 ** this.failedSessions;
        nextDelay = this.options.sessionCyclePeriodMs * factor;
      } else {
        // Session continues: thread the model's next state.
        this.state = result.nextState;
        this.failedSessions = 0;
      }
    } catch {
      // A failed cycle (e.g. a transient peer error) must not kill the loop;
      // keep the current state and retry next period.
    } finally {
      this.running = false;
      if (!this.stopped) this.scheduleNext(nextDelay);
    }
  }

  /** Attribute per-silo dispersed (shed) and acquired (received) counts for the report. */
  private updateReportCounts(moves: readonly MigrationMove[] | undefined): void {
    if (moves === undefined) {
      this.dispersed = EMPTY_REPORT_COUNTS;
      this.acquired = EMPTY_REPORT_COUNTS;
      return;
    }
    const dispersed = new Map<string, number>();
    const acquired = new Map<string, number>();
    for (const move of moves) {
      dispersed.set(move.from, (dispersed.get(move.from) ?? 0) + move.count);
      acquired.set(move.to, (acquired.get(move.to) ?? 0) + move.count);
    }
    this.dispersed = dispersed;
    this.acquired = acquired;
  }
}
