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

  const activations = meter.createObservableGauge("tsva.activations", {
    description: "Live grain activations on this silo",
    unit: "{activation}",
  });
  const activationsCb = (result: ObservableResult): void =>
    result.observe(sources.activationCount());
  activations.addCallback(activationsCb);
  unregister.push(() => activations.removeCallback(activationsCb));

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
    const hitsCb = (result: ObservableResult): void => result.observe(directoryCache().hits);
    const missesCb = (result: ObservableResult): void => result.observe(directoryCache().misses);
    hits.addCallback(hitsCb);
    misses.addCallback(missesCb);
    unregister.push(
      () => hits.removeCallback(hitsCb),
      () => misses.removeCallback(missesCb),
    );
  }

  return () => unregister.forEach((fn) => fn());
}
