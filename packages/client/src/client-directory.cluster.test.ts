import { describe, it } from "vitest";
import { clientId } from "@thresh/core/client-grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";
import { waitFor } from "@thresh/testing/wait";

const CLUSTER = "c1";
const silo1Addr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
const silo2Addr = new SiloAddress("silo-2", "uid-2", "silo-2:11112");
const clientAddr = new SiloAddress("client", "uid-c", "client:22222");

function buildTwoSiloCluster(network: InProcessNetwork) {
  const addresses = [silo1Addr, silo2Addr];
  const membership1 = new StaticMembershipService(silo1Addr, addresses);
  const membership2 = new StaticMembershipService(silo2Addr, addresses);
  const silo1 = new ClusterNode({
    local: silo1Addr,
    clusterId: CLUSTER,
    membership: membership1,
    transport: new InProcessTransport(network, CLUSTER),
  });
  const silo2 = new ClusterNode({
    local: silo2Addr,
    clusterId: CLUSTER,
    membership: membership2,
    transport: new InProcessTransport(network, CLUSTER),
  });
  return { silo1, silo2, membership2 };
}

describe("client directory: gateway gossip across a cluster", () => {
  it("records the connecting client on its gateway and gossips it to every other silo", async () => {
    const network = new InProcessNetwork();
    const { silo1, silo2 } = buildTwoSiloCluster(network);
    await silo1.start();
    await silo2.start();

    const fixedClientId = clientId("c-int");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: silo1Addr,
      clientId: fixedClientId,
    });
    await client.connect();

    try {
      await waitFor(() => silo1.clientGatewayFor(fixedClientId)?.equals(silo1Addr) === true);
      await waitFor(() => silo2.clientGatewayFor(fixedClientId)?.equals(silo1Addr) === true);
    } finally {
      await client.close();
      await silo1.stop();
      await silo2.stop();
    }
  });

  it("drops a departed gateway from every silo's client directory view", async () => {
    const network = new InProcessNetwork();
    const { silo1, silo2, membership2 } = buildTwoSiloCluster(network);
    await silo1.start();
    await silo2.start();

    const fixedClientId = clientId("c-leave");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: silo1Addr,
      clientId: fixedClientId,
    });
    await client.connect();

    try {
      await waitFor(() => silo2.clientGatewayFor(fixedClientId)?.equals(silo1Addr) === true);

      // silo1 leaves the cluster's view (mirrors membership marking it dead).
      membership2.removeSilo(silo1Addr);
      silo2.updateView();

      await waitFor(() => silo2.clientGatewayFor(fixedClientId) === undefined);
    } finally {
      await client.close();
      await silo1.stop();
      await silo2.stop();
    }
  });
});
