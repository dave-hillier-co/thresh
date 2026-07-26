import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";

/**
 * Options for transaction-rate load shedding (Orleans
 * `TransactionRateLoadSheddingOptions`). When `enabled`, the
 * {@link TransactionOverloadDetector} sheds transaction starts once the
 * estimated start rate exceeds `limit` transactions/second.
 */
export interface TransactionRateLoadSheddingOptions {
  /** Whether transaction load shedding is on. Defaults to `false`. */
  enabled: boolean;
  /** Load-shedding limit in transactions/second (Orleans `Limit`). */
  limit: number;
}

/** Orleans `TransactionRateLoadSheddingOptions.DEFAULT_LIMIT`. */
export const DEFAULT_TRANSACTION_RATE_LIMIT = 700;

export function defaultTransactionRateLoadSheddingOptions(): TransactionRateLoadSheddingOptions {
  return { enabled: false, limit: DEFAULT_TRANSACTION_RATE_LIMIT };
}

/**
 * Running counts of the transaction agent's activity (Orleans
 * `ITransactionAgentStatistics`). Only `transactionsStarted` participates in
 * overload detection; the other counters mirror upstream's surface.
 */
export interface TransactionAgentStatistics {
  readonly transactionsStarted: number;
  readonly transactionsSucceeded: number;
  readonly transactionsFailed: number;
  readonly transactionsThrottled: number;
  trackTransactionStarted(): void;
  trackTransactionSucceeded(): void;
  trackTransactionFailed(): void;
  trackTransactionThrottled(): void;
}

export class TransactionAgentStatisticsImpl implements TransactionAgentStatistics {
  private started = 0;
  private succeeded = 0;
  private failed = 0;
  private throttled = 0;

  get transactionsStarted(): number {
    return this.started;
  }
  get transactionsSucceeded(): number {
    return this.succeeded;
  }
  get transactionsFailed(): number {
    return this.failed;
  }
  get transactionsThrottled(): number {
    return this.throttled;
  }

  trackTransactionStarted(): void {
    this.started++;
  }
  trackTransactionSucceeded(): void {
    this.succeeded++;
  }
  trackTransactionFailed(): void {
    this.failed++;
  }
  trackTransactionThrottled(): void {
    this.throttled++;
  }
}

/**
 * Rate-based transaction load-shedding detector — a faithful port of Orleans'
 * `TransactionOverloadDetector`. Every {@link METRICS_CHECK_MS} it snapshots
 * the started-transaction counter to record the achieved rate over the last
 * window; `isOverloaded()` blends that recorded rate with the instantaneous
 * rate since the last snapshot (a decaying estimate, weighting the current
 * window 2:1) and returns `true` once it exceeds the configured limit. The
 * clock is injected so tests drive it deterministically instead of racing a
 * real stopwatch.
 */
export class TransactionOverloadDetector {
  /** Orleans `MetricsCheck` = 15 seconds. */
  static readonly METRICS_CHECK_MS = 15_000;

  private lastStartedSnapshot: number;
  private transactionStartedPerSecond = 0;
  private lastCheckTimeMs: number;
  private lastMonitorMs: number;

  constructor(
    private readonly statistics: TransactionAgentStatistics,
    private readonly options: TransactionRateLoadSheddingOptions,
    private readonly time: TimeProvider = systemTimeProvider,
  ) {
    const now = this.time.now();
    this.lastStartedSnapshot = statistics.transactionsStarted;
    this.lastCheckTimeMs = now;
    this.lastMonitorMs = now;
  }

  /** Snapshot the achieved rate over the window that just elapsed (Orleans `RecordStatistics`). */
  private recordStatistics(nowMs: number): void {
    const currentStarted = this.statistics.transactionsStarted;
    this.transactionStartedPerSecond = calculateTps(
      this.lastStartedSnapshot,
      this.lastCheckTimeMs,
      currentStarted,
      nowMs,
    );
    this.lastStartedSnapshot = currentStarted;
    this.lastCheckTimeMs = nowMs;
  }

  isOverloaded(): boolean {
    if (!this.options.enabled) return false;

    const now = this.time.now();
    // Orleans `PeriodicAction.TryAction`: run the 15s snapshot when due.
    if (now - this.lastMonitorMs >= TransactionOverloadDetector.METRICS_CHECK_MS) {
      this.recordStatistics(now);
      this.lastMonitorMs = now;
    }

    const txPerSecondCurrently = calculateTps(
      this.lastStartedSnapshot,
      this.lastCheckTimeMs,
      this.statistics.transactionsStarted,
      now,
    );
    // Decaying utilization: weight the current window 2:1 against the last.
    const aggregatedTxPerSecond =
      (this.transactionStartedPerSecond + 2.0 * txPerSecondCurrently) / 3.0;

    return aggregatedTxPerSecond > this.options.limit;
  }
}

/**
 * Transactions/second over a window (Orleans `TransactionOverloadDetector.CalculateTps`):
 * under a second of elapsed time the raw delta count is treated as the rate,
 * so a burst in the first fraction of a second is not divided down to a tiny
 * rate; otherwise the delta is scaled to per-second.
 */
function calculateTps(
  startCounter: number,
  startTimeMs: number,
  currentCounter: number,
  currentTimeMs: number,
): number {
  const deltaTimeMs = currentTimeMs - startTimeMs;
  const deltaCounter = currentCounter - startCounter;
  return deltaTimeMs < 1000 ? deltaCounter : (deltaCounter * 1000.0) / deltaTimeMs;
}
