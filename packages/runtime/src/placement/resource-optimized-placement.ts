import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type {
  PlacementContext,
  PlacementStrategy,
  SiloResourceStats,
} from "@thresh/runtime/placement/placement-strategy";
import { pickRandom } from "@thresh/runtime/placement/placement-strategy";
import {
  defaultResourceOptimizedPlacementOptions,
  type ResourceOptimizedPlacementOptions,
} from "@thresh/runtime/placement/resource-optimized-placement-options";

interface NormalizedWeights {
  cpuUsage: number;
  memoryUsage: number;
  availableMemory: number;
  maxAvailableMemory: number;
  activationCount: number;
}

/**
 * Places the grain on the silo its resource readings and weights favour (Orleans
 * `ResourceOptimizedPlacementDirector`). Each candidate that has reported statistics and is not
 * overloaded gets a score in [0, 1], lower being better:
 *
 *   w.cpu * cpu/100 + w.memoryUsage * memoryUsage/max + w.availableMemory * (1 - available/max)
 *     + w.maxAvailableMemory * (1 - max/largestMax) + w.activationCount * count/largestCount
 *
 * with the weights normalised to sum to 1 (the memory terms apply only when the silo reports a
 * maximum). The lowest score wins, unless the local silo scores within
 * `localSiloPreferenceMargin` of it, in which case the local silo is kept.
 *
 * Differences from Orleans: the readings are the raw ones a silo pushes, not Kalman-filtered
 * (Orleans scores `FilteredCpuUsagePercentage` and the filtered memory figures); every eligible
 * candidate is scored, where Orleans scores a random sqrt(n) subset (power of K); a candidate
 * with no statistics is scored from its activation count alone if the context has one, rather
 * than skipped; and with every reporting silo overloaded the first candidate is chosen, as in
 * Orleans.
 */
export class ResourceOptimizedPlacement implements PlacementStrategy {
  private readonly weights: NormalizedWeights;
  private readonly localSiloPreferenceMargin: number;

  constructor(
    options: ResourceOptimizedPlacementOptions = defaultResourceOptimizedPlacementOptions(),
  ) {
    this.weights = normalizeWeights(options);
    this.localSiloPreferenceMargin = options.localSiloPreferenceMargin / 100;
  }

  choose(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    ctx: PlacementContext,
  ): SiloAddress {
    if (candidates.length === 0) throw new Error("no placement candidates");
    if (candidates.length === 1) return candidates[0]!;
    const random = ctx.random ?? Math.random;

    const known: Array<{ silo: SiloAddress; stats: SiloResourceStats }> = [];
    for (const silo of candidates) {
      const stats = statsOf(silo, ctx);
      if (stats !== undefined) known.push({ silo, stats });
    }
    if (known.length === 0) return pickRandom(candidates, random);

    let largestMaxMemory = 0;
    let largestActivationCount = 0;
    for (const { stats } of known) {
      largestMaxMemory = Math.max(largestMaxMemory, stats.maximumAvailableMemoryBytes ?? 0);
      largestActivationCount = Math.max(largestActivationCount, stats.activationCount ?? 0);
    }
    const score = (stats: SiloResourceStats): number =>
      this.score(stats, largestMaxMemory, largestActivationCount);

    let best: { silo: SiloAddress; score: number } | undefined;
    let localScore: number | undefined;
    for (const { silo, stats } of known) {
      if (stats.isOverloaded === true) continue;
      const s = score(stats);
      // Jitter so equal scores do not always pick the first (Orleans `scoreJitter`).
      if (best === undefined || s + random() / 100_000 < best.score) best = { silo, score: s };
      if (silo.equals(ctx.localSilo)) localScore = s;
    }
    if (best === undefined) return candidates[0]!;
    if (localScore === undefined || localScore - this.localSiloPreferenceMargin > best.score) {
      return best.silo;
    }
    return candidates.find((c) => c.equals(ctx.localSilo))!;
  }

  private score(
    stats: SiloResourceStats,
    largestMaxMemory: number,
    largestActivationCount: number,
  ): number {
    const w = this.weights;
    let score = w.cpuUsage * ((stats.cpuUsagePercentage ?? 0) / 100);
    const max = stats.maximumAvailableMemoryBytes ?? 0;
    if (max > 0) {
      const memoryUsage = (stats.memoryUsageBytes ?? 0) / max;
      const availableMemory = (stats.availableMemoryBytes ?? 0) / max;
      score +=
        w.memoryUsage * memoryUsage +
        w.availableMemory * (1 - availableMemory) +
        w.maxAvailableMemory * (1 - max / largestMaxMemory);
    }
    if (largestActivationCount > 0) {
      score += w.activationCount * ((stats.activationCount ?? 0) / largestActivationCount);
    }
    return score;
  }
}

function statsOf(silo: SiloAddress, ctx: PlacementContext): SiloResourceStats | undefined {
  const reported = ctx.resourceStats?.(silo);
  const overloaded = reported?.isOverloaded ?? ctx.isOverloaded?.(silo);
  if (reported !== undefined) {
    const activationCount = reported.activationCount ?? ctx.activationCount?.(silo);
    return {
      ...reported,
      ...(activationCount !== undefined ? { activationCount } : {}),
      ...(overloaded !== undefined ? { isOverloaded: overloaded } : {}),
    };
  }
  const activationCount = ctx.activationCount?.(silo);
  if (activationCount === undefined) return undefined;
  return { activationCount, ...(overloaded !== undefined ? { isOverloaded: overloaded } : {}) };
}

function normalizeWeights(options: ResourceOptimizedPlacementOptions): NormalizedWeights {
  const total =
    options.cpuUsageWeight +
    options.memoryUsageWeight +
    options.availableMemoryWeight +
    options.maxAvailableMemoryWeight +
    options.activationCountWeight;
  if (total === 0) {
    return {
      cpuUsage: 0,
      memoryUsage: 0,
      availableMemory: 0,
      maxAvailableMemory: 0,
      activationCount: 0,
    };
  }
  return {
    cpuUsage: options.cpuUsageWeight / total,
    memoryUsage: options.memoryUsageWeight / total,
    availableMemory: options.availableMemoryWeight / total,
    maxAvailableMemory: options.maxAvailableMemoryWeight / total,
    activationCount: options.activationCountWeight / total,
  };
}
