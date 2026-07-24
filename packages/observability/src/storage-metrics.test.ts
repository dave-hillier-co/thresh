import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { withStorageOpMetrics } from "@tsva/observability/storage-metrics";

const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 });
const meterProvider = new MeterProvider({ readers: [reader] });
metrics.setGlobalMeterProvider(meterProvider);

async function collected(): Promise<
  { name: string; dataPoints: readonly { value: unknown; attributes: Record<string, unknown> }[] }[]
> {
  await meterProvider.forceFlush();
  return exporter
    .getMetrics()
    .flatMap((rm) => rm.scopeMetrics)
    .flatMap((sm) => sm.metrics)
    .map((m) => ({ name: m.descriptor.name, dataPoints: m.dataPoints }));
}

describe("withStorageOpMetrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("records an ok op counter and a duration sample on success", async () => {
    const result = await withStorageOpMetrics(
      { provider: "MemoryGrainStorage", stateName: "s", grainId: "g1", operation: "read" },
      async () => "value",
    );
    expect(result).toBe("value");

    const recorded = await collected();
    const ops = recorded.find((m) => m.name === "tsva.storage.ops");
    const duration = recorded.find((m) => m.name === "tsva.storage.op.duration");
    expect(ops).toBeDefined();
    expect(duration).toBeDefined();
    expect(ops!.dataPoints[0]!.attributes["tsva.status"]).toBe("ok");
    expect(ops!.dataPoints[0]!.attributes["tsva.storage.operation"]).toBe("read");
  });

  it("records an error status and rethrows on failure", async () => {
    await expect(
      withStorageOpMetrics(
        { provider: "MemoryGrainStorage", stateName: "s", grainId: "g1", operation: "write" },
        async () => {
          throw new Error("boom");
        },
      ),
    ).rejects.toThrow("boom");

    const recorded = await collected();
    const ops = recorded.find((m) => m.name === "tsva.storage.ops");
    const errored = ops!.dataPoints.find((dp) => dp.attributes["tsva.status"] === "error");
    expect(errored).toBeDefined();
  });
});
