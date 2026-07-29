import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { AlertPublisherGrain } from "@thresh/example-broadcast/alert-publisher-grain";
import { AuditLogGrain } from "@thresh/example-broadcast/audit-log-grain";
import { RegionMonitorGrain } from "@thresh/example-broadcast/region-monitor-grain";
import { IAlertPublisher, IAuditLog, IRegionMonitor } from "@thresh/example-broadcast/interfaces";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

/** A silo with a broadcast-channel provider and the alert grains registered. */
export function buildBroadcastSilo(): SiloHost {
  return createSilo({ clusterId: "alerts", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useBroadcastChannels()
    .registerGrain(AlertPublisherGrain.grain, { interfaces: [IAlertPublisher] })
    .registerGrain(RegionMonitorGrain.grain, { interfaces: [IRegionMonitor] })
    .registerGrain(AuditLogGrain.grain, { interfaces: [IAuditLog] })
    .build();
}

export interface BroadcastDemoResult {
  /** Alert texts each region monitor received. */
  monitors: Record<string, string[]>;
  /** Audit-log entries each region's audit grain recorded. */
  audits: Record<string, string[]>;
}

/**
 * Publish alerts to two regions and report what each region's monitor and audit
 * log received — showing cluster-wide fan-out to implicit subscribers, multi-type
 * fan-out on one channel key, and key-based isolation (eu alerts never reach us).
 */
export async function runBroadcastDemo(): Promise<BroadcastDemoResult> {
  const silo = buildBroadcastSilo();
  await silo.start();
  try {
    const publisher = silo.getGrain(IAlertPublisher, "ops");
    await publisher.raise("eu", "disk pressure");
    await publisher.raise("eu", "node drained");
    await publisher.raise("us", "latency spike");

    const monitors: Record<string, string[]> = {};
    const audits: Record<string, string[]> = {};
    for (const region of ["eu", "us"]) {
      monitors[region] = (await silo.getGrain(IRegionMonitor, region).alerts()).map((a) => a.text);
      audits[region] = await silo.getGrain(IAuditLog, region).entries();
    }
    return { monitors, audits };
  } finally {
    await silo.stop();
  }
}
