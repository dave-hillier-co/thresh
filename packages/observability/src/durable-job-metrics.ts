import { metrics, type Attributes, type Counter, type ObservableResult } from "@opentelemetry/api";

/**
 * OpenTelemetry instruments for durable-job execution (`@tsva/durable-jobs`):
 * `tsva.durablejobs.started`/`.completed`/`.failed` counters for a job
 * attempt's outcome, and `tsva.durablejobs.queue.depth` — an observable gauge
 * over the jobs currently queued in this silo's shard executors. Emitted
 * through the global OpenTelemetry meter — a no-op until the host registers
 * an SDK.
 *
 * Instruments are created lazily on first use, not at module load — see
 * `storage-metrics.ts` for why (the metrics API is not a late-binding proxy).
 */
interface Instruments {
  started: Counter;
  completed: Counter;
  failed: Counter;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@tsva/observability");
    instruments = {
      started: meter.createCounter("tsva.durablejobs.started", {
        description: "Durable job attempts started",
        unit: "{job}",
      }),
      completed: meter.createCounter("tsva.durablejobs.completed", {
        description: "Durable job attempts that completed successfully",
        unit: "{job}",
      }),
      failed: meter.createCounter("tsva.durablejobs.failed", {
        description: "Durable job attempts that failed",
        unit: "{job}",
      }),
    };
  }
  return instruments;
}

export function recordJobStarted(attrs: Attributes = {}): void {
  instrumentsOf().started.add(1, attrs);
}

export function recordJobCompleted(attrs: Attributes = {}): void {
  instrumentsOf().completed.add(1, attrs);
}

export function recordJobFailed(attrs: Attributes = {}): void {
  instrumentsOf().failed.add(1, attrs);
}

/**
 * Register an observable gauge sampling `queueDepth()` — the number of jobs
 * queued across this silo's shard executors. Returns a function that
 * unregisters it (call it when the manager stops).
 */
export function registerDurableJobQueueDepth(queueDepth: () => number): () => void {
  const meter = metrics.getMeter("@tsva/observability");
  const gauge = meter.createObservableGauge("tsva.durablejobs.queue.depth", {
    description: "Jobs queued across this silo's durable-job shard executors",
    unit: "{job}",
  });
  const callback = (result: ObservableResult): void => result.observe(queueDepth());
  gauge.addCallback(callback);
  return () => gauge.removeCallback(callback);
}
