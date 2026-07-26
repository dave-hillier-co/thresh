import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  recordAgentPoll,
  recordStreamDelivered,
  recordStreamFailed,
} from "@thresh/observability/stream-metrics";

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

describe("stream metrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("counts delivered events, failed events, and agent polls independently", async () => {
    recordStreamDelivered({ "thresh.stream.key": "s1" });
    recordStreamFailed({ "thresh.stream.key": "s1" });
    recordAgentPoll();
    recordAgentPoll();

    const delivered = await metric("thresh.streams.delivered");
    const failed = await metric("thresh.streams.failed");
    const polls = await metric("thresh.streams.agent.polls");
    expect(delivered!.dataPoints[0]!.value).toBe(1);
    expect(failed!.dataPoints[0]!.value).toBe(1);
    expect(polls!.dataPoints[0]!.value).toBe(2);
  });
});
