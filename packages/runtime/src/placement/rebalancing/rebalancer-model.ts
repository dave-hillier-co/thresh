/**
 * The activation rebalancer's decision model — a faithful, pure port
 * of Orleans' adaptive, entropy-minimizing rebalancing
 * (`Orleans.Runtime/Placement/Rebalancing/ActivationRebalancerWorker.cs`; algorithm:
 * https://www.ledjonbehluli.com/posts/orleans_adaptive_rebalancing/).
 *
 * Each silo's load is modelled as a single scalar (its activation count): the
 * uniform-memory case of Orleans' `(memory, activations)` weighting, since this
 * runtime does not yet gossip per-silo memory. The
 * load distribution is treated as a probability distribution whose Shannon
 * entropy we drive toward the maximum (`ln S`, the perfectly even split),
 * migrating a scaled, bounded number of activations from busy to quiet silos each
 * cycle and stopping when balanced or stagnant.
 *
 * Everything here is pure and deterministic — no cluster, no clock, no I/O — so
 * the (harder) distributed wiring in slice 2 builds on a verified core.
 */

/** Tunables, mirroring `ActivationRebalancerOptions`. */
export interface RebalancerOptions {
  /** Consecutive low-improvement cycles before a session gives up (inclusive). */
  maxStagnantCycles: number;
  /** Minimum normalized entropy change counted as an improvement. */
  entropyQuantum: number;
  /** Imbalance at or below which the cluster is treated as even (session complete). */
  allowedEntropyDeviation: number;
  /** Scale `allowedEntropyDeviation` up for very large clusters. */
  scaleAllowedEntropyDeviation: boolean;
  /** Activation count above which the allowed-deviation scaling kicks in. */
  scaledEntropyDeviationActivationThreshold: number;
  /** Weight of the cycle number in adaptive scaling (higher ⇒ faster migration). */
  cycleNumberWeight: number;
  /** Weight of the silo count in adaptive scaling (higher ⇒ slower migration). */
  siloNumberWeight: number;
  /** Maximum activations migrated per silo pair per cycle. */
  activationMigrationCountLimit: number;
}

/** Named defaults, mirroring `ActivationRebalancerOptions`' `DEFAULT_*`/`MAX_*` static fields. */
export const DEFAULT_MAX_STAGNANT_CYCLES = 3;
export const DEFAULT_ENTROPY_QUANTUM = 0.0001;
export const DEFAULT_ALLOWED_ENTROPY_DEVIATION = 0.0001;
export const DEFAULT_SCALE_ALLOWED_ENTROPY_DEVIATION = true;
/** The cap `ComputeAllowedEntropyDeviation` scales up to (Orleans `MAX_SCALED_ENTROPY_DEVIATION`). */
export const MAX_SCALED_ENTROPY_DEVIATION = 0.1;
export const DEFAULT_SCALED_ENTROPY_DEVIATION_ACTIVATION_THRESHOLD = 10_000;
export const DEFAULT_CYCLE_NUMBER_WEIGHT = 0.1;
export const DEFAULT_SILO_NUMBER_WEIGHT = 0.1;
/** Practically "no limit" — mirrors Orleans' `int.MaxValue` default within this codebase's number range. */
export const DEFAULT_ACTIVATION_MIGRATION_COUNT_LIMIT = Number.MAX_SAFE_INTEGER;

export const DEFAULT_REBALANCER_OPTIONS: RebalancerOptions = {
  maxStagnantCycles: DEFAULT_MAX_STAGNANT_CYCLES,
  entropyQuantum: DEFAULT_ENTROPY_QUANTUM,
  allowedEntropyDeviation: DEFAULT_ALLOWED_ENTROPY_DEVIATION,
  scaleAllowedEntropyDeviation: DEFAULT_SCALE_ALLOWED_ENTROPY_DEVIATION,
  scaledEntropyDeviationActivationThreshold: DEFAULT_SCALED_ENTROPY_DEVIATION_ACTIVATION_THRESHOLD,
  cycleNumberWeight: DEFAULT_CYCLE_NUMBER_WEIGHT,
  siloNumberWeight: DEFAULT_SILO_NUMBER_WEIGHT,
  activationMigrationCountLimit: DEFAULT_ACTIVATION_MIGRATION_COUNT_LIMIT,
};

/** The rebalancer's per-session state, carried between cycles. */
export interface CycleState {
  /** Cycles run so far this session (drives adaptive scaling). */
  rebalancingCycle: number;
  /** Consecutive low-improvement cycles so far this session. */
  stagnantCycles: number;
  /** Entropy computed in the previous cycle (0 at session start). */
  previousEntropy: number;
}

export const INITIAL_CYCLE_STATE: CycleState = {
  rebalancingCycle: 0,
  stagnantCycles: 0,
  previousEntropy: 0,
};

/** Why a session ended after a cycle, if it did. */
export type StopReason = "complete" | "stagnated";

/** One migration the cycle wants: move `count` activations `from` a busy silo `to` a quiet one. */
export interface MigrationMove {
  from: string;
  to: string;
  count: number;
}

/** The outcome of planning one rebalancing cycle. */
export interface CyclePlan {
  /** Whether the cycle ran (false when there are too few silos to balance). */
  ran: boolean;
  /** Migrations to perform this cycle (empty on a skipped, stagnant, or terminal cycle). */
  moves: MigrationMove[];
  /** The cluster imbalance `(maxEntropy − entropy) / maxEntropy ∈ [0,1]` (0 when not run). */
  imbalance: number;
  /** State to feed the next cycle. */
  nextState: CycleState;
  /** Set when this cycle ends the session. */
  stop?: StopReason;
}

const EPSILON = 1e-10;

/**
 * Shannon entropy `−Σ p_i ln p_i` of the load distribution `p_i = load_i / Σload`.
 * With uniform memory this is exactly Orleans' normalized-ratio entropy (the
 * memory term cancels under normalization). Returns 0 for an empty or zero-total
 * snapshot.
 */
export function shannonEntropy(loads: readonly number[]): number {
  let total = 0;
  for (const load of loads) total += load;
  if (total <= 0) return 0;
  let entropy = 0;
  for (const load of loads) {
    const p = load / total;
    const v = Math.max(p, EPSILON); // avoid log(0)
    entropy -= v * Math.log(v);
  }
  return entropy;
}

/** The cluster imbalance: how far the distribution is from even, in `[0,1]`. */
export function clusterImbalance(loads: readonly number[]): number {
  const siloCount = loads.length;
  if (siloCount < 2) return 0;
  const maxEntropy = Math.log(siloCount);
  return (maxEntropy - shannonEntropy(loads)) / maxEntropy;
}

/**
 * Orleans' adaptive scaling factor: `(1 − e^(−cycleWeight·cycle)) · 1/(1 +
 * siloWeight·(S−1))`. Ramps the migration rate up over a session's cycles
 * (slow start, then accelerate) while damping it for larger clusters.
 */
export function adaptiveScaling(
  siloCount: number,
  cycle: number,
  options: RebalancerOptions,
): number {
  const cycleFactor = 1 - Math.exp(-options.cycleNumberWeight * cycle);
  const siloFactor = 1 / (1 + options.siloNumberWeight * (siloCount - 1));
  return cycleFactor * siloFactor;
}

/**
 * Pair silos for migration: sort by load (ties broken by id for determinism),
 * then pair the least-loaded with the most-loaded, working inward. An odd silo
 * out (the median) is left unpaired this cycle.
 */
export function formSiloPairs(snapshot: ReadonlyMap<string, number>): Array<[string, string]> {
  const sorted = [...snapshot.entries()].sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : 1));
  const pairs: Array<[string, string]> = [];
  let left = 0;
  let right = sorted.length - 1;
  while (left < right) {
    pairs.push([sorted[left]![0], sorted[right]![0]]);
    left++;
    right--;
  }
  return pairs;
}

/** Orleans' `ComputeAllowedEntropyDeviation`: scale the base deviation up for very large clusters. */
function allowedDeviation(totalActivations: number, options: RebalancerOptions): number {
  if (
    !options.scaleAllowedEntropyDeviation ||
    totalActivations < options.scaledEntropyDeviationActivationThreshold
  ) {
    return options.allowedEntropyDeviation;
  }
  const logFactor = Math.trunc(
    Math.log10(totalActivations / options.scaledEntropyDeviationActivationThreshold),
  );
  return Math.min(
    options.allowedEntropyDeviation * Math.pow(10, logFactor),
    MAX_SCALED_ENTROPY_DEVIATION,
  );
}

/**
 * Plan one rebalancing cycle from the current per-silo load `snapshot` and the
 * carried `state`. Mirrors `RunRebalancingCycle`: skip if fewer than two silos;
 * stop the session if it has stagnated for `maxStagnantCycles` or if the cluster
 * is balanced; otherwise count a stagnant cycle when entropy barely changed, or
 * compute per-pair migrations scaled by `alpha · adaptiveScaling`.
 */
export function planCycle(
  snapshot: ReadonlyMap<string, number>,
  options: RebalancerOptions,
  state: CycleState,
): CyclePlan {
  const siloCount = snapshot.size;
  if (siloCount < 2) {
    return { ran: false, moves: [], imbalance: 0, nextState: state };
  }

  const loads = [...snapshot.values()];
  let total = 0;
  for (const load of loads) total += load;
  if (total <= 0) {
    return { ran: false, moves: [], imbalance: 0, nextState: state };
  }

  const cycle = state.rebalancingCycle + 1;
  const maxEntropy = Math.log(siloCount);
  const entropy = shannonEntropy(loads);
  const imbalance = (maxEntropy - entropy) / maxEntropy;

  // The session has stagnated for too long: give up (the worker will back off).
  if (state.stagnantCycles >= options.maxStagnantCycles) {
    return {
      ran: true,
      moves: [],
      imbalance,
      nextState: {
        rebalancingCycle: cycle,
        stagnantCycles: state.stagnantCycles,
        previousEntropy: entropy,
      },
      stop: "stagnated",
    };
  }

  // The cluster is effectively even: the session is done.
  if (imbalance < allowedDeviation(total, options)) {
    return { ran: true, moves: [], imbalance, nextState: INITIAL_CYCLE_STATE, stop: "complete" };
  }

  // Too little improvement to be worthwhile: count a stagnant cycle and cool down.
  const entropyChange = Math.abs((entropy - state.previousEntropy) / maxEntropy);
  if (entropyChange < options.entropyQuantum) {
    return {
      ran: true,
      moves: [],
      imbalance,
      nextState: {
        rebalancingCycle: cycle,
        stagnantCycles: state.stagnantCycles + 1,
        previousEntropy: entropy,
      },
    };
  }

  const alpha = entropy / maxEntropy;
  const scaling = adaptiveScaling(siloCount, cycle, options);
  const ideal = total / siloCount;
  const moves: MigrationMove[] = [];

  for (const [lowSilo, highSilo] of formSiloPairs(snapshot)) {
    const lowLoad = snapshot.get(lowSilo)!;
    const highLoad = snapshot.get(highSilo)!;
    const difference = Math.abs(lowLoad - ideal - (highLoad - ideal));
    let delta = Math.trunc(alpha * scaling * (difference / 2));
    if (delta === 0) continue;
    if (delta > highLoad) delta = highLoad;
    if (delta > options.activationMigrationCountLimit)
      delta = options.activationMigrationCountLimit;
    moves.push({ from: highSilo, to: lowSilo, count: delta });
  }

  return {
    ran: true,
    moves,
    imbalance,
    nextState: { rebalancingCycle: cycle, stagnantCycles: 0, previousEntropy: entropy },
  };
}
