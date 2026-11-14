import os from "node:os";
import { performance, type EventLoopUtilization } from "node:perf_hooks";

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

/**
 * One reading of the silo's environment (Orleans `EnvironmentStatistics`, the value
 * `IEnvironmentStatisticsProvider.GetEnvironmentStatistics` returns).
 */
export interface EnvironmentStatistics {
  /** CPU utilisation, 0-100. */
  cpuUsagePercentage: number;
  /** Memory the process currently uses, in bytes. */
  memoryUsageBytes: number;
  /** Memory still available to the process, in bytes. */
  availableMemoryBytes: number;
  /** The most memory the process can use, in bytes. */
  maximumAvailableMemoryBytes: number;
}

/**
 * The silo's statistics source (Orleans `IEnvironmentStatisticsProvider`). Inject one through
 * the silo builder; the default is `NodeEnvironmentStatisticsProvider`. Implementations are
 * read on demand and must not start timers that outlive the silo.
 */
export interface EnvironmentStatisticsProvider {
  getEnvironmentStatistics(): EnvironmentStatistics;
}

/**
 * The default provider: Node's event-loop utilisation as the CPU reading, and process and OS
 * memory for the rest. Each read reports utilisation over the window since the previous read
 * (`perf_hooks.performance.eventLoopUtilization(previous)`), so there is no sampling timer to
 * leak; the first read covers the process lifetime. Unlike Orleans' provider the readings are
 * raw, not Kalman-filtered.
 */
export class NodeEnvironmentStatisticsProvider implements EnvironmentStatisticsProvider {
  private previous: EventLoopUtilization | undefined;
  private cached: { at: number; statistics: EnvironmentStatistics } | undefined;

  getEnvironmentStatistics(): EnvironmentStatistics {
    const now = performance.now();
    // The gateway asks on every client request: a reading younger than the minimum window is
    // reused, which also keeps each CPU window long enough to mean something.
    if (this.cached !== undefined && now - this.cached.at < MIN_READING_WINDOW_MS) {
      return this.cached.statistics;
    }
    const maximum = maximumAvailableMemoryBytes();
    const statistics: EnvironmentStatistics = {
      cpuUsagePercentage: this.readCpuUsagePercentage(),
      memoryUsageBytes: Math.min(process.memoryUsage.rss(), maximum),
      availableMemoryBytes: Math.min(Math.max(os.freemem(), 1), maximum),
      maximumAvailableMemoryBytes: maximum,
    };
    this.cached = { at: now, statistics };
    return statistics;
  }

  private readCpuUsagePercentage(): number {
    const current = performance.eventLoopUtilization();
    const window =
      this.previous === undefined ? current : performance.eventLoopUtilization(current, this.previous);
    this.previous = current;
    return Math.min(100, Math.max(0, window.utilization * 100));
  }
}

const MIN_READING_WINDOW_MS = 100;

/** The cgroup limit when the process has one, otherwise physical memory. */
function maximumAvailableMemoryBytes(): number {
  const constrained = process.constrainedMemory();
  const total = os.totalmem();
  return constrained > 0 && constrained < total ? constrained : total;
}

/**
 * Wraps a real provider with a test-only CPU override (Orleans
 * `TestHooksEnvironmentStatisticsProvider`): while latched, the CPU reading is the latched
 * value and every other reading passes through from the wrapped provider.
 */
export class TestHooksEnvironmentStatisticsProvider implements EnvironmentStatisticsProvider {
  private latchedCpuUsagePercentage: number | undefined;

  constructor(private readonly inner: EnvironmentStatisticsProvider) {}

  getEnvironmentStatistics(): EnvironmentStatistics {
    const live = this.inner.getEnvironmentStatistics();
    return this.latchedCpuUsagePercentage === undefined
      ? live
      : { ...live, cpuUsagePercentage: this.latchedCpuUsagePercentage };
  }

  /** Force the reported CPU usage to `value` until `unlatchHardwareStatistics()`. */
  latchCpuUsage(value: number): void {
    this.latchedCpuUsagePercentage = value;
  }

  /** Clear a latch set by `latchCpuUsage`, reverting to the wrapped provider's reading. */
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
/**
 * A silo's load-shedding test-hook surface (Orleans `IPlacementTestGrain`'s
 * Latch/Unlatch methods). The four latch/unlatch operations return a promise
 * that resolves once the new state has been pushed to every other live silo
 * (Orleans' test-only `PropagateStatisticsToCluster`, forcing an immediate
 * `DeploymentLoadPublisher` gossip round rather than waiting for the next
 * periodic interval) — awaiting it is what makes the change visible to
 * load-aware placement decisions made on a peer silo right after.
 */
export interface SiloLoadSheddingTestHooks {
  enableOverloadDetection(enabled: boolean): void;
  latchCpuUsage(value: number): Promise<void>;
  unlatchCpuUsage(): Promise<void>;
  latchOverloaded(): Promise<void>;
  unlatchOverloaded(): Promise<void>;
}

export class OverloadDetector {
  /** Whether overload detection is active (Orleans `IOverloadDetector.Enabled`, toggled by `EnableOverloadDetection`). */
  enabled: boolean;

  constructor(
    private readonly statistics: EnvironmentStatisticsProvider,
    private readonly options: LoadSheddingOptions,
  ) {
    this.enabled = options.loadSheddingEnabled;
  }

  get isOverloaded(): boolean {
    if (!this.enabled) return false;
    return (
      this.statistics.getEnvironmentStatistics().cpuUsagePercentage > this.options.cpuThreshold
    );
  }
}
