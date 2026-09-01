import { describe, expect, it } from "vitest";
import { IManagementGrain } from "@thresh/core/management-grain";
import { InProcessTransport } from "@thresh/messaging/in-process-transport";
import { TestCluster } from "@thresh/testing/test-cluster";
import { createClient } from "@thresh/client/client-node";

/**
 * A client addresses a grain by mapping its (erased) interface onto a grain type, which
 * `registerGrains` learns from an application grain's own class. The BUILT-IN system grains have no
 * such class to register from — `ManagementGrain` is built per silo, closing over that silo's
 * context — so without a standing mapping a client cannot call the one grain every Orleans cluster
 * client can call, and topology questions ("which silos are up?") become silo-only.
 */
describe("a client addressing a built-in system grain", () => {
  it("calls IManagementGrain with nothing registered", async () => {
    const cluster = await TestCluster.start({
      clusterId: "client-system-grain-cluster",
      initialSilos: 2,
    });
    const client = createClient({
      clusterId: cluster.clusterId,
      transport: new InProcessTransport(cluster.network, cluster.clusterId),
      gateway: cluster.primary.address,
    });
    await client.connect();
    try {
      const hosts = await client.getGrain(IManagementGrain, 0n).getHosts(true);
      expect(hosts.size).toBe(2);
    } finally {
      await client.close();
      await cluster.dispose();
    }
  });
});
