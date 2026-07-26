import { metrics, type Attributes } from "@opentelemetry/api";
import type { IncomingGrainCallFilter } from "@thresh/core/grain-call-filter";

/**
 * A grain call filter that records OpenTelemetry metrics for incoming calls: a
 * `thresh.grain.calls` counter (by interface, method, and ok/error status) and a
 * `thresh.grain.call.duration` histogram (milliseconds). Emitted through the global
 * OpenTelemetry meter — a no-op until the host registers an SDK. Pairs with the
 * tracing filter.
 */
export function metricsFilters(): { incoming: IncomingGrainCallFilter } {
  const meter = metrics.getMeter("@thresh/observability");
  const calls = meter.createCounter("thresh.grain.calls", {
    description: "Number of grain method calls",
    unit: "{call}",
  });
  const duration = meter.createHistogram("thresh.grain.call.duration", {
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
      calls.add(1, { ...attrs, "thresh.status": status });
    }
  };

  return { incoming };
}
