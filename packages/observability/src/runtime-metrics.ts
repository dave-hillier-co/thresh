import { metrics, type ObservableResult } from "@opentelemetry/api";

/** Live silo values the observable instruments sample at collection time. */
export interface RuntimeMetricSources {
  /** Number of live grain activations hosted on this silo. */
  activationCount: () => number;
  /** Cumulative location-cache hits/misses, for the directory hit-rate metric. */
  directoryCache?: () => { hits: number; misses: number };
  /**
   * Cumulative directory range-recovery outcomes: entries adopted by a recovery
   * pull, and source pulls whose retry budget ran out (those ranges run on lazy
   * reactivation until the re-armed pull gets through, so a non-zero rate is the
   * signal that the directory is working harder than it should).
   */
  directoryRecovery?: () => { recovered: number; exhausted: number };
}

/**
 * Register OpenTelemetry observable instruments for silo runtime state that a
 * call filter cannot capture — `thresh.activations` (live activation count, a
 * gauge) and `thresh.directory.cache.hits`/`.misses` (cumulative counters).
 * No-op without an SDK.
 * Returns a function that unregisters them (call it when the silo stops).
 */
export function registerRuntimeMetrics(sources: RuntimeMetricSources): () => void {
  const meter = metrics.getMeter("@thresh/observability");
  const unregister: Array<() => void> = [];

  type ObservableInstrument = {
    addCallback: (cb: (result: ObservableResult) => void) => void;
    removeCallback: (cb: (result: ObservableResult) => void) => void;
  };
  const registerObservableInstrument = (
    instrument: ObservableInstrument,
    callback: (result: ObservableResult) => void,
  ): void => {
    instrument.addCallback(callback);
    unregister.push(() => instrument.removeCallback(callback));
  };

  const activations = meter.createObservableGauge("thresh.activations", {
    description: "Live grain activations on this silo",
    unit: "{activation}",
  });
  registerObservableInstrument(activations, (result) => result.observe(sources.activationCount()));

  const { directoryCache } = sources;
  if (directoryCache !== undefined) {
    const hits = meter.createObservableCounter("thresh.directory.cache.hits", {
      description: "Location-cache hits",
      unit: "{lookup}",
    });
    const misses = meter.createObservableCounter("thresh.directory.cache.misses", {
      description: "Location-cache misses",
      unit: "{lookup}",
    });
    const makeObservableCb =
      (field: "hits" | "misses") =>
      (result: ObservableResult): void =>
        result.observe(directoryCache()[field]);
    registerObservableInstrument(hits, makeObservableCb("hits"));
    registerObservableInstrument(misses, makeObservableCb("misses"));
  }

  const { directoryRecovery } = sources;
  if (directoryRecovery !== undefined) {
    const recovered = meter.createObservableCounter("thresh.directory.recovery.recovered", {
      description: "Directory entries adopted by a range recovery",
      unit: "{entry}",
    });
    const exhausted = meter.createObservableCounter("thresh.directory.recovery.exhausted", {
      description: "Directory range-recovery sources whose retry budget ran out",
      unit: "{source}",
    });
    registerObservableInstrument(recovered, (result) =>
      result.observe(directoryRecovery().recovered),
    );
    registerObservableInstrument(exhausted, (result) =>
      result.observe(directoryRecovery().exhausted),
    );
  }

  return () => unregister.forEach((fn) => fn());
}
