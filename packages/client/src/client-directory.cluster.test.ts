import { describe, expect, it } from "vitest";
import { clientId } from "@thresh/core/client-grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";
import { waitFor } from "@thresh/testing/wait";

const CLUSTER = "c1";
const silo1Addr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
const silo2Addr = new SiloAddress("silo-2", "uid-2", "silo-2:11112");
const silo3Addr = new SiloAddress("silo-3", "uid-3", "silo-3:11113");

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

  it("keeps a briefly disconnected client registered, then drops it and gossips unregister once ClientDropTimeout elapses with no reconnect", async () => {
    const network = new InProcessNetwork();
    const time = new FakeTimeProvider();
    const addresses = [silo1Addr, silo2Addr];
    const membership1 = new StaticMembershipService(silo1Addr, addresses);
    const membership2 = new StaticMembershipService(silo2Addr, addresses);
    const silo1 = new ClusterNode({
      local: silo1Addr,
      clusterId: CLUSTER,
      membership: membership1,
      transport: new InProcessTransport(network, CLUSTER),
      time,
      clientDropTimeoutMs: 1_000,
    });
    const silo2 = new ClusterNode({
      local: silo2Addr,
      clusterId: CLUSTER,
      membership: membership2,
      transport: new InProcessTransport(network, CLUSTER),
      time,
      clientDropTimeoutMs: 1_000,
    });
    await silo1.start();
    await silo2.start();

    const fixedClientId = clientId("c-drop");
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: silo1Addr,
      clientId: fixedClientId,
    });
    await client.connect();

    try {
      await waitFor(() => silo2.clientGatewayFor(fixedClientId)?.equals(silo1Addr) === true);

      await client.close();

      // Still within the grace period: the gateway keeps routing to itself for
      // this client (Orleans keeps `ClientState` until `ReadyToDrop`, in case
      // of a quick reconnect), even though its local socket is already gone.
      time.advance(500);
      expect(silo1.clientGatewayFor(fixedClientId)?.equals(silo1Addr)).toBe(true);

      // Past ClientDropTimeout with no reconnect: the next maintenance sweep
      // drops it locally and gossips `unregister` to every peer.
      time.advance(600);
      await waitFor(() => silo1.clientGatewayFor(fixedClientId) === undefined);
      await waitFor(() => silo2.clientGatewayFor(fixedClientId) === undefined);
    } finally {
      await silo1.stop();
      await silo2.stop();
    }
  });

  it("republishes an existing client to a silo that joins the cluster after it connected", async () => {
    const network = new InProcessNetwork();
    const membership1 = new StaticMembershipService(silo1Addr, [silo1Addr, silo2Addr]);
    const membership2 = new StaticMembershipService(silo2Addr, [silo1Addr, silo2Addr]);
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
    await silo1.start();
    await silo2.start();

    const fixedClientId = clientId("c-late-join");
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: silo1Addr,
      clientId: fixedClientId,
    });
    await client.connect();

    // silo3 joins only after the client already connected: `broadcastClientGossip`
    // at accept time never reached it, so without a membership-triggered
    // republish it would answer any call to this client with "no gateway".
    const membership3 = new StaticMembershipService(silo3Addr, [silo1Addr, silo3Addr]);
    const silo3 = new ClusterNode({
      local: silo3Addr,
      clusterId: CLUSTER,
      membership: membership3,
      transport: new InProcessTransport(network, CLUSTER),
    });
    await silo3.start();

    try {
      await waitFor(() => silo1.clientGatewayFor(fixedClientId)?.equals(silo1Addr) === true);

      membership1.addSilo(silo3Addr);
      silo1.updateView();

      await waitFor(() => silo3.clientGatewayFor(fixedClientId)?.equals(silo1Addr) === true);
    } finally {
      await client.close();
      await silo1.stop();
      await silo2.stop();
      await silo3.stop();
    }
  });
});
