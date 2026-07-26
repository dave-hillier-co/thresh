import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

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

      const calls = recorded.find((m) => m.descriptor.name === "thresh.grain.calls");
      const duration = recorded.find((m) => m.descriptor.name === "thresh.grain.call.duration");
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
        .find((m) => m.descriptor.name === "thresh.activations");
      expect(gauge).toBeDefined();
      expect(gauge!.dataPoints.at(-1)!.value as number).toBeGreaterThanOrEqual(2);
    } finally {
      await silo.stop();
    }
  });

  it("reports directory location-cache hit/miss counters", async () => {
    const silo = createSilo({ clusterId: "metrics-cache", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMetrics()
      .registerGrain(GreeterGrain, { interfaces: [Greeter] })
      .build();
    await silo.start();
    try {
      // Repeated calls to one key drive directory lookups (a miss then hits).
      await silo.getGrain(Greeter, "g").greet("a");
      await silo.getGrain(Greeter, "g").greet("b");
      await silo.getGrain(Greeter, "g").greet("c");

      await meterProvider.forceFlush();
      const recorded = exporter
        .getMetrics()
        .flatMap((rm) => rm.scopeMetrics)
        .flatMap((sm) => sm.metrics);
      const hits = recorded.find((m) => m.descriptor.name === "thresh.directory.cache.hits");
      const misses = recorded.find((m) => m.descriptor.name === "thresh.directory.cache.misses");
      expect(hits).toBeDefined();
      expect(misses).toBeDefined();
      const total =
        (hits!.dataPoints.at(-1)!.value as number) + (misses!.dataPoints.at(-1)!.value as number);
      expect(total).toBeGreaterThanOrEqual(1);
    } finally {
      await silo.stop();
    }
  });
});
