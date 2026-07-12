/**
 * Load shedding (Orleans `LoadSheddingOptions` + `OverloadDetector` +
 * `TestHooksEnvironmentStatisticsProvider`, `src/Orleans.Core/Configuration/Options/LoadSheddingOptions.cs`
 * and `src/Orleans.Runtime/Messaging/OverloadDetector.cs`). A silo tracks its
 * own reported CPU usage; once load shedding is enabled and that usage
 * crosses `cpuThreshold`, the silo is "overloaded" and its gateway path
 * refuses new client-originated requests with a `GatewayTooBusyException`
 * (see `ClusterNode.receiveRequest`) rather than queuing behind an already
 * struggling process.
 */

/** Mirrors Orleans `LoadSheddingOptions` (memory-threshold field omitted: nothing here reports memory usage). */
export interface LoadSheddingOptions {
  /** Whether load shedding is enforced at all. Defaults to `false` (Orleans parity: opt-in). */
  loadSheddingEnabled: boolean;
  /** CPU utilization (0-100) at which this silo starts shedding load. Defaults to 95. */
  cpuThreshold: number;
}

export const DEFAULT_LOAD_SHEDDING_OPTIONS: LoadSheddingOptions = {
  loadSheddingEnabled: false,
  cpuThreshold: 95,
};

export interface EnvironmentStatistics {
  cpuUsagePercentage: number;
}

/**
 * A silo's CPU-usage source with a test-only override (Orleans
 * `TestHooksEnvironmentStatisticsProvider`). This framework has no real
 * OS-level CPU sampler wired in yet, so the un-latched reading is a steady
 * `0` (never overloaded on its own) until a test latches a value.
 */
export class TestHooksEnvironmentStatisticsProvider {
  private latchedCpuUsagePercentage: number | undefined;

  getEnvironmentStatistics(): EnvironmentStatistics {
    return { cpuUsagePercentage: this.latchedCpuUsagePercentage ?? 0 };
  }

  /** Force the reported CPU usage to `value` until `unlatchHardwareStatistics()`. */
  latchCpuUsage(value: number): void {
    this.latchedCpuUsagePercentage = value;
  }

  /** Clear a latch set by `latchCpuUsage`, reverting to the live (currently always-0) reading. */
  unlatchHardwareStatistics(): void {
    this.latchedCpuUsagePercentage = undefined;
  }
}

/**
 * Determines whether this silo is overloaded (Orleans `OverloadDetector`):
 * disabled entirely unless `LoadSheddingOptions.loadSheddingEnabled`, and
 * otherwise true once the reported CPU usage exceeds `cpuThreshold`. Upstream
 * caches the reading for a second between checks; this port re-reads on every
 * access instead — the statistics provider here is in-memory and latch-driven,
 * so there is no sampling cost to amortise, and always-fresh keeps the two
 * load-shedding tests' immediate latch/unlatch toggling observable without a
 * `ForceRefresh()` step.
 */
/** A silo's load-shedding test-hook surface (Orleans `IPlacementTestGrain`'s Latch/Unlatch methods). */
export interface SiloLoadSheddingTestHooks {
  enableOverloadDetection(enabled: boolean): void;
  latchCpuUsage(value: number): void;
  unlatchCpuUsage(): void;
  latchOverloaded(): void;
  unlatchOverloaded(): void;
}

export class OverloadDetector {
  /** Whether overload detection is active (Orleans `IOverloadDetector.Enabled`, toggled by `EnableOverloadDetection`). */
  enabled: boolean;

  constructor(
    private readonly statistics: TestHooksEnvironmentStatisticsProvider,
    private readonly options: LoadSheddingOptions,
  ) {
    this.enabled = options.loadSheddingEnabled;
  }

  get isOverloaded(): boolean {
    if (!this.enabled) return false;
    return this.statistics.getEnvironmentStatistics().cpuUsagePercentage > this.options.cpuThreshold;
  }
}
