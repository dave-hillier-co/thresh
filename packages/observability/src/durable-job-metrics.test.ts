import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  recordJobCompleted,
  recordJobFailed,
  recordJobStarted,
  registerDurableJobQueueDepth,
} from "@tsva/observability/durable-job-metrics";

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

describe("durable-job metrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("counts started, completed, and failed attempts independently", async () => {
    recordJobStarted({ "tsva.job.name": "j" });
    recordJobStarted({ "tsva.job.name": "j" });
    recordJobCompleted({ "tsva.job.id": "1" });
    recordJobFailed({ "tsva.job.id": "2" });

    const started = await metric("tsva.durablejobs.started");
    const completed = await metric("tsva.durablejobs.completed");
    const failed = await metric("tsva.durablejobs.failed");
    expect(started!.dataPoints[0]!.value).toBe(2);
    expect(completed!.dataPoints[0]!.value).toBe(1);
    expect(failed!.dataPoints[0]!.value).toBe(1);
  });

  it("samples the queue-depth observable gauge from the given callback", async () => {
    const unregister = registerDurableJobQueueDepth(() => 7);
    try {
      const gauge = await metric("tsva.durablejobs.queue.depth");
      expect(gauge!.dataPoints[0]!.value).toBe(7);
    } finally {
      unregister();
    }
  });
});
