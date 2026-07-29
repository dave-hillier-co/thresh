import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { AlertPublisherGrain } from "@thresh/example-broadcast/alert-publisher-grain";
import { AuditLogGrain } from "@thresh/example-broadcast/audit-log-grain";
import { RegionMonitorGrain } from "@thresh/example-broadcast/region-monitor-grain";
import { runBroadcastDemo } from "@thresh/example-broadcast/demo";
import { IAlertPublisher, IAuditLog, IRegionMonitor } from "@thresh/example-broadcast/interfaces";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(): SiloHost {
  return createSilo({ clusterId: "alerts", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useBroadcastChannels()
    .registerGrain(AlertPublisherGrain.grain, { interfaces: [IAlertPublisher] })
    .registerGrain(RegionMonitorGrain.grain, { interfaces: [IRegionMonitor] })
    .registerGrain(AuditLogGrain.grain, { interfaces: [IAuditLog] })
    .build();
}

describe("broadcast channels (pub/sub fan-out acceptance)", () => {
  it("fans an alert out to every grain type subscribed to the channel's region", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      await silo.getGrain(IAlertPublisher, "ops").raise("eu", "disk pressure");
      // Both the region monitor and the region audit log — distinct grain types
      // implicitly subscribed to "alerts" — receive the same publish.
      const monitor = await silo.getGrain(IRegionMonitor, "eu").alerts();
      const audit = await silo.getGrain(IAuditLog, "eu").entries();
      expect(monitor.map((a) => a.text)).toEqual(["disk pressure"]);
      expect(audit).toEqual(["[eu] disk pressure"]);
    } finally {
      await silo.stop();
    }
  });

  it("isolates channels by key — a region hears only its own alerts", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      await silo.getGrain(IAlertPublisher, "ops").raise("eu", "eu-only");
      expect((await silo.getGrain(IRegionMonitor, "eu").alerts()).map((a) => a.text)).toEqual([
        "eu-only",
      ]);
      // The us monitor was never published to, so it has seen nothing.
      expect(await silo.getGrain(IRegionMonitor, "us").alerts()).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("runs the runnable demo end-to-end", async () => {
    const result = await runBroadcastDemo();
    expect(result.monitors.eu).toEqual(["disk pressure", "node drained"]);
    expect(result.monitors.us).toEqual(["latency spike"]);
    expect(result.audits.eu).toEqual(["[eu] disk pressure", "[eu] node drained"]);
    expect(result.audits.us).toEqual(["[us] latency spike"]);
  });
});
