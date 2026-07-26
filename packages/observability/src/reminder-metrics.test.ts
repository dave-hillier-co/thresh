import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { recordReminderFired, recordReminderMissed } from "@thresh/observability/reminder-metrics";

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

describe("reminder metrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("counts fired and missed ticks separately", async () => {
    recordReminderFired({ "thresh.reminder.name": "r1" });
    recordReminderFired({ "thresh.reminder.name": "r1" });
    recordReminderMissed({ "thresh.reminder.name": "r2" });

    const fired = await metric("thresh.reminders.fired");
    const missed = await metric("thresh.reminders.missed");
    expect(fired!.dataPoints[0]!.value).toBe(2);
    expect(missed!.dataPoints[0]!.value).toBe(1);
  });
});
