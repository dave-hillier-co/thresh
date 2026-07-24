import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  recordMessageReceived,
  recordMessageSent,
  recordQueueLatency,
} from "@tsva/observability/messaging-metrics";

const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 });
const meterProvider = new MeterProvider({ readers: [reader] });
metrics.setGlobalMeterProvider(meterProvider);

async function metric(name: string) {
  await meterProvider.forceFlush();
  return exporter
    .getMetrics()
    .flatMap((rm) => rm.scopeMetrics)
    .flatMap((sm) => sm.metrics)
    .find((m) => m.descriptor.name === name);
}

describe("messaging metrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("counts sent and received messages", async () => {
    recordMessageSent({ "tsva.message.direction": "request" });
    recordMessageReceived({ "tsva.message.direction": "response" });

    const sent = await metric("tsva.messaging.sent");
    const received = await metric("tsva.messaging.received");
    expect(sent!.dataPoints[0]!.value).toBe(1);
    expect(received!.dataPoints[0]!.value).toBe(1);
  });

  it("records queue (connection-acquire) latency as a histogram", async () => {
    recordQueueLatency(12, { "tsva.peer": "silo-1:1000" });

    const latency = await metric("tsva.messaging.queue.latency");
    expect(latency).toBeDefined();
    expect(latency!.dataPoints[0]!.attributes["tsva.peer"]).toBe("silo-1:1000");
  });
});
