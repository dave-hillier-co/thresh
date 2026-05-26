import { metrics, type ObservableResult } from "@opentelemetry/api";

/** Live silo values an observable gauge samples at collection time. */
export interface RuntimeMetricSources {
  /** Number of live grain activations hosted on this silo. */
  activationCount: () => number;
}

/**
 * Register OpenTelemetry observable gauges for silo runtime state that a call
 * filter cannot capture — currently `tsva.activations` (the live activation
 * count), sampled from the catalog at collection time
 * ([ADR 0013](../../../docs/adr/0013-observability.md)). No-op without an SDK.
 * Returns a function that unregisters the gauges (call it when the silo stops).
 */
export function registerRuntimeMetrics(sources: RuntimeMetricSources): () => void {
  const meter = metrics.getMeter("@tsva/observability");
  const activations = meter.createObservableGauge("tsva.activations", {
    description: "Live grain activations on this silo",
    unit: "{activation}",
  });
  const callback = (result: ObservableResult): void => {
    result.observe(sources.activationCount());
  };
  activations.addCallback(callback);
  return () => activations.removeCallback(callback);
}
