import { metrics, type Attributes, type Counter, type Histogram } from "@opentelemetry/api";

/**
 * OpenTelemetry counter/histogram for persistent-state storage operations —
 * `thresh.storage.ops` (by provider, state name, operation, and ok/error status)
 * and `thresh.storage.op.duration` (milliseconds). Emitted alongside the
 * `withStorageReadSpan`/`withStorageWriteSpan` spans in `activation-tracing.ts`,
 * through the global OpenTelemetry meter — a no-op until the host registers an
 * SDK.
 *
 * Instruments are created lazily on first use (not at module load), since
 * `metrics.getMeter()` binds to whichever provider is global *at call time* —
 * unlike the trace API, it is not a late-binding proxy, so resolving it at
 * import time would freeze on a no-op meter if the host registers its SDK
 * afterward.
 */
interface Instruments {
  ops: Counter;
  duration: Histogram;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@thresh/observability");
    instruments = {
      ops: meter.createCounter("thresh.storage.ops", {
        description: "Number of grain storage operations",
        unit: "{operation}",
      }),
      duration: meter.createHistogram("thresh.storage.op.duration", {
        description: "Grain storage operation duration",
        unit: "ms",
      }),
    };
  }
  return instruments;
}

export interface StorageOpAttributes {
  provider: string;
  stateName: string;
  grainId: string;
  operation: "read" | "write" | "clear";
}

/** Runs `fn`, recording its outcome as a storage-op counter increment and duration sample. */
export async function withStorageOpMetrics<T>(
  attrs: StorageOpAttributes,
  fn: () => Promise<T>,
): Promise<T> {
  const { ops, duration } = instrumentsOf();
  const recorded: Attributes = {
    "orleans.storage.provider": attrs.provider,
    "orleans.storage.state.name": attrs.stateName,
    "orleans.grain.id": attrs.grainId,
    "thresh.storage.operation": attrs.operation,
  };
  const start = Date.now();
  let status = "ok";
  try {
    return await fn();
  } catch (err) {
    status = "error";
    throw err;
  } finally {
    duration.record(Date.now() - start, recorded);
    ops.add(1, { ...recorded, "thresh.status": status });
  }
}
