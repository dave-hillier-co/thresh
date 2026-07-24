import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  recordDirectoryLookup,
  recordDirectoryRegistration,
} from "@tsva/observability/directory-metrics";

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

describe("directory metrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("tags lookups and registrations with their locality", async () => {
    recordDirectoryLookup("local");
    recordDirectoryLookup("remote");
    recordDirectoryRegistration("local");

    const lookups = await metric("tsva.directory.lookups");
    const registrations = await metric("tsva.directory.registrations");
    const localities = lookups!.dataPoints.map((dp) => dp.attributes["tsva.directory.locality"]);
    expect(localities.sort()).toEqual(["local", "remote"]);
    expect(registrations!.dataPoints[0]!.attributes["tsva.directory.locality"]).toBe("local");
  });
});
