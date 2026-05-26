import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";

// Register a meter provider with an in-memory exporter before the silo builds
// (useMetrics resolves the global meter at build time).
const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 });
const meterProvider = new MeterProvider({ readers: [reader] });
metrics.setGlobalMeterProvider(meterProvider);

interface Greeter extends GrainWithStringKey {
  greet(name: string): Promise<string>;
}
const Greeter = defineGrainInterface<Greeter>("MetricsGreeter");

@grain({ name: "MetricsGreeter" })
class GreeterGrain extends Grain implements Greeter {
  async greet(name: string): Promise<string> {
    return `hello ${name}`;
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

describe("OpenTelemetry metrics filter", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("records a call counter and a duration histogram per grain call", async () => {
    const silo = createSilo({ clusterId: "metrics", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMetrics()
      .registerGrain(GreeterGrain, { interfaces: [Greeter] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(Greeter, "g").greet("a");
      await silo.getGrain(Greeter, "g").greet("b");

      await meterProvider.forceFlush();
      const recorded = exporter
        .getMetrics()
        .flatMap((rm) => rm.scopeMetrics)
        .flatMap((sm) => sm.metrics);

      const calls = recorded.find((m) => m.descriptor.name === "tsva.grain.calls");
      const duration = recorded.find((m) => m.descriptor.name === "tsva.grain.call.duration");
      expect(calls).toBeDefined();
      expect(duration).toBeDefined();

      const totalCalls = calls!.dataPoints.reduce((n, dp) => n + (dp.value as number), 0);
      expect(totalCalls).toBeGreaterThanOrEqual(2);
      expect(duration!.dataPoints.length).toBeGreaterThanOrEqual(1);
      expect(calls!.dataPoints[0]!.attributes["rpc.method"]).toBe("greet");
    } finally {
      await silo.stop();
    }
  });

  it("reports the live activation count as a gauge", async () => {
    const silo = createSilo({ clusterId: "metrics-gauge", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMetrics()
      .registerGrain(GreeterGrain, { interfaces: [Greeter] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(Greeter, "g1").greet("a");
      await silo.getGrain(Greeter, "g2").greet("b");

      await meterProvider.forceFlush();
      const gauge = exporter
        .getMetrics()
        .flatMap((rm) => rm.scopeMetrics)
        .flatMap((sm) => sm.metrics)
        .find((m) => m.descriptor.name === "tsva.activations");
      expect(gauge).toBeDefined();
      expect(gauge!.dataPoints.at(-1)!.value as number).toBeGreaterThanOrEqual(2);
    } finally {
      await silo.stop();
    }
  });
});
