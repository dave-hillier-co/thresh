import { describe, expect, it } from "vitest";
import type { LogFields, Logger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

interface LogRecord {
  level: string;
  message: string;
  fields?: LogFields;
}

function capturingLogger(records: LogRecord[]): Logger {
  const at =
    (level: string) =>
    (message: string, fields?: LogFields): void => {
      records.push({ level, message, ...(fields !== undefined ? { fields } : {}) });
    };
  return { debug: at("debug"), info: at("info"), warn: at("warn"), error: at("error") };
}

/** Let the membership watch's pending iterations run to completion. */
const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

describe("the membership watch", () => {
  it("keeps applying later views after a view update throws", async () => {
    // One view change's ownership reconciliation fails part-way through — the
    // durable-job store read is the reachable stand-in for a Redis/Postgres
    // blip, and `onOwnershipChange` hooks do real I/O against exactly those.
    const membership = new StaticMembershipService(local, [local]);
    const store = new MemoryJobShardStore();
    const listShards = store.listShards.bind(store);
    let storeUp = true;
    store.listShards = (): ReturnType<MemoryJobShardStore["listShards"]> =>
      storeUp ? listShards() : Promise.reject(new Error("job store unavailable"));
    const records: LogRecord[] = [];

    const host: SiloHost = createSilo({ clusterId: "c", local })
      .useMembership(membership)
      .useInProcessTransport(new InProcessNetwork())
      .useMemoryDurableJobs(store)
      .useLogging(capturingLogger(records))
      .build();
    await host.start();
    try {
      expect(host.health.ready().checks.membershipHealthy).toBe(true);

      storeUp = false;
      membership.setSilos([local]); // ownership reconciliation throws here
      await settle();
      expect(
        records.some((r) => r.level === "error" && r.message.includes("membership view")),
      ).toBe(true);

      // The watch must still be watching: the next view is applied, and the
      // empty active set is reported (a silo frozen on the last view it managed
      // to apply goes on claiming this silo is a member of a cluster that has
      // no members at all).
      storeUp = true;
      membership.setSilos([]);
      await settle();
      expect(host.health.ready().checks.membershipHealthy).toBe(false);

      membership.setSilos([local]);
      await settle();
      expect(host.health.ready().checks.membershipHealthy).toBe(true);
    } finally {
      await host.stop();
    }
  });
});
