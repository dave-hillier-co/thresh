import { afterAll, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { noopLogger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeEndpointSliceApi, type FakePod } from "@thresh/clustering-k8s/fake-endpoint-slice-api";
import {
  KubernetesEndpointWatch,
  type EndpointSliceSource,
} from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import { waitFor } from "@thresh/testing/wait";

// Registered before the silo builds: `useMetrics` resolves the global meter at build time.
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

describe("membership metrics on a Kubernetes-membership silo", () => {
  afterAll(async () => {
    await meterProvider.shutdown();
  });

  it("report the applied view and counts, move when the view changes, and count watch failures", async () => {
    const api = new FakeEndpointSliceApi();
    const pod = (n: number, ready: boolean): FakePod => ({
      name: `silo-${n}`,
      uid: `uid-${n}`,
      ip: `10.0.0.${n + 1}`,
      ready,
    });
    api.setPod(pod(0, true));
    const address = new SiloAddress("silo-0", "uid-0", api.endpointOf(pod(0, true)));

    let failing = false;
    let dropWatch: ((err?: unknown) => void) | undefined;
    const inner = api.source();
    const source: EndpointSliceSource = {
      list: () => (failing ? Promise.reject(new Error("api down")) : inner.list()),
      watch: (resourceVersion, onEvent, onClose) => {
        dropWatch = onClose;
        return inner.watch(resourceVersion, onEvent);
      },
    };
    const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10, random: () => 0 });
    const host = createSilo({
      clusterId: "membership-metrics",
      local: address,
      logger: noopLogger,
      selfProbe: { enabled: false },
      gracefulShutdownMs: 0,
    })
      .useKubernetesMembership(watch, { portName: "silo" })
      .useInProcessTransport(new InProcessNetwork())
      .useMetrics()
      .build();
    await host.start();
    try {
      await waitFor(async () => (await latest("thresh.membership.members.active")) === 1);
      const firstVersion = await latest("thresh.membership.view.version");
      expect(firstVersion).toBeGreaterThan(0);
      expect(await latest("thresh.membership.members")).toBe(1);
      expect(await latest("thresh.membership.watch.reconnect.failures")).toBe(0);
      expect(await latest("thresh.membership.watch.last_sync.age")).toBeGreaterThanOrEqual(0);

      // A peer joins: the view changes and the metrics move with it.
      api.setPod(pod(1, true));
      await waitFor(async () => (await latest("thresh.membership.members.active")) === 2);
      expect(await latest("thresh.membership.members")).toBe(2);
      expect(await latest("thresh.membership.view.version")).toBeGreaterThan(firstVersion!);

      // The API goes away: the watch drops, relists fail, and the counter climbs.
      failing = true;
      dropWatch?.(new Error("connection reset"));
      await waitFor(
        async () => ((await latest("thresh.membership.watch.reconnect.failures")) ?? 0) >= 2,
      );
    } finally {
      await host.stop();
    }
  });
});
