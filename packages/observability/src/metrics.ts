import { metrics, type Attributes } from "@opentelemetry/api";
import type { IncomingGrainCallFilter } from "@tsva/core/grain-call-filter";

/**
 * A grain call filter that records OpenTelemetry metrics for incoming calls: a
 * `tsva.grain.calls` counter (by interface, method, and ok/error status) and a
 * `tsva.grain.call.duration` histogram (milliseconds). Emitted through the global
 * OpenTelemetry meter — a no-op until the host registers an SDK. Pairs with the
 * tracing filter ([ADR 0013](../../../docs/adr/0013-observability.md)).
 */
export function metricsFilters(): { incoming: IncomingGrainCallFilter } {
  const meter = metrics.getMeter("@tsva/observability");
  const calls = meter.createCounter("tsva.grain.calls", {
    description: "Number of grain method calls",
    unit: "{call}",
  });
  const duration = meter.createHistogram("tsva.grain.call.duration", {
    description: "Grain method call duration",
    unit: "ms",
  });

  const incoming: IncomingGrainCallFilter = async (ctx) => {
    const attrs: Attributes = { "rpc.service": ctx.interfaceName, "rpc.method": ctx.methodName };
    const start = Date.now();
    let status = "ok";
    try {
      await ctx.invoke();
    } catch (err) {
      status = "error";
      throw err;
    } finally {
      duration.record(Date.now() - start, attrs);
      calls.add(1, { ...attrs, "tsva.status": status });
    }
  };

  return { incoming };
}
