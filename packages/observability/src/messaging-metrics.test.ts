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
} from "@thresh/observability/messaging-metrics";

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
    recordMessageSent({ "thresh.message.direction": "request" });
    recordMessageReceived({ "thresh.message.direction": "response" });

    const sent = await metric("thresh.messaging.sent");
    const received = await metric("thresh.messaging.received");
    expect(sent!.dataPoints[0]!.value).toBe(1);
    expect(received!.dataPoints[0]!.value).toBe(1);
  });

  it("records queue (connection-acquire) latency as a histogram", async () => {
    recordQueueLatency(12, { "thresh.peer": "silo-1:1000" });

    const latency = await metric("thresh.messaging.queue.latency");
    expect(latency).toBeDefined();
    expect(latency!.dataPoints[0]!.attributes["thresh.peer"]).toBe("silo-1:1000");
  });
});
