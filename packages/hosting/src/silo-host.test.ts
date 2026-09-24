import { describe, expect, it } from "vitest";
import { GracefulShutdown } from "@thresh/hosting/graceful-shutdown";
import { HealthCheck } from "@thresh/hosting/health-check";
import { SiloHost, type SiloHostParts } from "@thresh/hosting/silo-host";
import type { ClusterNode } from "@thresh/runtime/cluster-node";

function buildHost(order: string[], onBeforeDeactivate: Array<() => Promise<void>>): SiloHost {
  const health = new HealthCheck();
  const node = {
    stop: async () => {
      order.push("node.stop (deactivateAll)");
    },
  } as unknown as ClusterNode;
  const shutdown = new GracefulShutdown(health, node, { delay: () => Promise.resolve() });
  const parts: SiloHostParts = {
    node,
    serviceId: "svc",
    health,
    healthServer: undefined,
    healthPort: undefined,
    shutdown,
    membership: {
      current: () => ({ silos: [], version: 0 }),
      updates: async function* () {},
    } as never,
    onBeforeDeactivate,
    onStop: [
      async () => {
        order.push("onStop");
      },
    ],
  };
  return new SiloHost(parts);
}

describe("SiloHost.stop ordering (issue #108)", () => {
  it("runs onBeforeDeactivate hooks (stream providers, durable-job manager) before the node deactivates activations", async () => {
    const order: string[] = [];
    const host = buildHost(order, [
      async () => {
        order.push("stream provider stop");
      },
      async () => {
        order.push("durable job manager stop");
      },
    ]);

    await host.stop();

    expect(order).toEqual([
      "stream provider stop",
      "durable job manager stop",
      "node.stop (deactivateAll)",
      "onStop",
    ]);
  });

  it("still drains the node and runs onStop when an onBeforeDeactivate hook throws", async () => {
    // These hooks now run FIRST: a provider whose stop() fails must not skip
    // deactivating every activation, closing transport and the onStop teardown
    // (Orleans logs a failed lifecycle stop and carries on stopping).
    const order: string[] = [];
    const host = buildHost(order, [
      async () => {
        throw new Error("provider stop failed");
      },
      async () => {
        order.push("durable job manager stop");
      },
    ]);

    await host.stop();

    expect(order).toEqual(["durable job manager stop", "node.stop (deactivateAll)", "onStop"]);
  });
});
