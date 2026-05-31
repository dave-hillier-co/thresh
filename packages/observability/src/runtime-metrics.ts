import { metrics, type ObservableResult } from "@opentelemetry/api";

/** Live silo values the observable instruments sample at collection time. */
export interface RuntimeMetricSources {
  /** Number of live grain activations hosted on this silo. */
  activationCount: () => number;
  /** Cumulative location-cache hits/misses, for the directory hit-rate metric. */
  directoryCache?: () => { hits: number; misses: number };
}

/**
 * Register OpenTelemetry observable instruments for silo runtime state that a
 * call filter cannot capture — `tsva.activations` (live activation count, a
 * gauge) and `tsva.directory.cache.hits`/`.misses` (cumulative counters).
 * No-op without an SDK.
 * Returns a function that unregisters them (call it when the silo stops).
 */
export function registerRuntimeMetrics(sources: RuntimeMetricSources): () => void {
  const meter = metrics.getMeter("@tsva/observability");
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

  const activations = meter.createObservableGauge("tsva.activations", {
    description: "Live grain activations on this silo",
    unit: "{activation}",
  });
  registerObservableInstrument(activations, (result) => result.observe(sources.activationCount()));

  const { directoryCache } = sources;
  if (directoryCache !== undefined) {
    const hits = meter.createObservableCounter("tsva.directory.cache.hits", {
      description: "Location-cache hits",
      unit: "{lookup}",
    });
    const misses = meter.createObservableCounter("tsva.directory.cache.misses", {
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

  return () => unregister.forEach((fn) => fn());
}
