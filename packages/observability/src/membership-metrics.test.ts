import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { registerMembershipMetrics } from "@thresh/observability/membership-metrics";

const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 });
const meterProvider = new MeterProvider({ readers: [reader] });
metrics.setGlobalMeterProvider(meterProvider);

async function latest(name: string): Promise<number | undefined> {
  exporter.reset();
  await meterProvider.forceFlush();
  const metric = exporter
    .getMetrics()
    .flatMap((rm) => rm.scopeMetrics)
    .flatMap((sm) => sm.metrics)
    .find((m) => m.descriptor.name === name);
  return metric?.dataPoints.at(-1)?.value as number | undefined;
}

describe("membership metrics", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  // First, deliberately: the exporter is cumulative, so once any test has reported an age it
  // keeps exporting it; this one asserts the gauge is absent when no sync has happened.
  it("reports no sync age before the watch has ever synced", async () => {
    const unregister = registerMembershipMetrics({
      appliedViewVersion: () => 0,
      memberCount: () => 0,
      activeMemberCount: () => 0,
      watch: () => ({ reconnectFailures: 1, lastSyncedAtMs: undefined, nowMs: 5_000 }),
    });
    try {
      expect(await latest("thresh.membership.watch.last_sync.age")).toBeUndefined();
      expect(await latest("thresh.membership.watch.reconnect.failures")).toBe(1);
    } finally {
      unregister();
    }
  });

  it("reports the applied view version and the member counts, and moves with them", async () => {
    let view = { version: 7, members: 3, active: 2 };
    const unregister = registerMembershipMetrics({
      appliedViewVersion: () => view.version,
      memberCount: () => view.members,
      activeMemberCount: () => view.active,
    });
    try {
      expect(await latest("thresh.membership.view.version")).toBe(7);
      expect(await latest("thresh.membership.members")).toBe(3);
      expect(await latest("thresh.membership.members.active")).toBe(2);

      view = { version: 9, members: 4, active: 4 };
      expect(await latest("thresh.membership.view.version")).toBe(9);
      expect(await latest("thresh.membership.members")).toBe(4);
      expect(await latest("thresh.membership.members.active")).toBe(4);
    } finally {
      unregister();
    }
  });

  it("reports the watch's reconnect failures and the seconds since it last synced", async () => {
    let watch = { reconnectFailures: 0, lastSyncedAtMs: 1_000 as number | undefined, nowMs: 4_500 };
    const unregister = registerMembershipMetrics({
      appliedViewVersion: () => 1,
      memberCount: () => 1,
      activeMemberCount: () => 1,
      watch: () => watch,
    });
    try {
      expect(await latest("thresh.membership.watch.reconnect.failures")).toBe(0);
      expect(await latest("thresh.membership.watch.last_sync.age")).toBe(3.5);

      watch = { reconnectFailures: 2, lastSyncedAtMs: 1_000, nowMs: 61_000 };
      expect(await latest("thresh.membership.watch.reconnect.failures")).toBe(2);
      expect(await latest("thresh.membership.watch.last_sync.age")).toBe(60);
    } finally {
      unregister();
    }
  });
});
