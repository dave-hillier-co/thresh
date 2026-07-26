import { createServer } from "node:net";
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";
import { membershipGatewayProvider, staticGatewayProvider } from "@thresh/client/gateway-provider";

/** Ask the OS for a free TCP port so silos and the client don't collide on one. */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const info = probe.address();
      const port = typeof info === "object" && info !== null ? info.port : 0;
      probe.close(() => resolve(port));
    });
  });
}

interface IEcho extends GrainWithStringKey {
  echo(message: string): Promise<string>;
}
const IEcho = defineGrainInterface<IEcho>("IEcho.gateway");

@grain()
class EchoGrain extends Grain implements IEcho {
  async echo(message: string): Promise<string> {
    return message;
  }
}

const CLUSTER = "c1";
const clientAddr = new SiloAddress("client", "uid-c", "client:22222");

describe("client gateway discovery + failover (in-process)", () => {
  it("skips an unreachable gateway and routes the call through a live one", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-0", "uid-0", "silo-0:11110");
    const dead = new SiloAddress("dead", "uid-dead", "dead:9999"); // never listens
    const silo = new ClusterNode({
      local: siloAddr,
      clusterId: CLUSTER,
      membership: new StaticMembershipService(siloAddr, [siloAddr]),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0,
    });
    silo.registerGrain(EchoGrain, { interfaces: [IEcho] });
    await silo.start();
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      // The dead gateway is tried first (round-robin); the client must fail over.
      gateways: staticGatewayProvider([dead, siloAddr]),
    }).registerGrain(EchoGrain, { interfaces: [IEcho] });
    await client.connect();
    try {
      expect(await client.getGrain(IEcho, "x").echo("hello")).toBe("hello");
      // The live gateway keeps serving once the dead one is out of rotation.
      expect(await client.getGrain(IEcho, "y").echo("again")).toBe("again");
    } finally {
      await client.close();
      await silo.stop();
    }
  });

  it("discovers gateways from cluster membership", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-0", "uid-0", "silo-0:11110");
    const membership = new StaticMembershipService(siloAddr, [siloAddr]);
    const silo = new ClusterNode({
      local: siloAddr,
      clusterId: CLUSTER,
      membership,
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0,
    });
    silo.registerGrain(EchoGrain, { interfaces: [IEcho] });
    await silo.start();
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateways: membershipGatewayProvider(membership),
    }).registerGrain(EchoGrain, { interfaces: [IEcho] });
    await client.connect();
    try {
      expect(await client.getGrain(IEcho, "x").echo("via-membership")).toBe("via-membership");
    } finally {
      await client.close();
      await silo.stop();
    }
  });
});

describe("client over real WebSocket sockets", () => {
  it("routes a call through a membership-discovered gateway", async () => {
    const siloPort = await freePort();
    const clientPort = await freePort();
    const siloAddr = new SiloAddress("silo-0", "uid-0", `127.0.0.1:${siloPort}`);
    const membership = new StaticMembershipService(siloAddr, [siloAddr]);
    const silo = new ClusterNode({
      local: siloAddr,
      clusterId: CLUSTER,
      membership,
      transport: new WebSocketTransport(CLUSTER),
      random: () => 0,
    });
    silo.registerGrain(EchoGrain, { interfaces: [IEcho] });
    await silo.start();
    const client = createClient({
      clusterId: CLUSTER,
      local: new SiloAddress("client", "uid-c", `127.0.0.1:${clientPort}`),
      transport: new WebSocketTransport(CLUSTER),
      gateways: membershipGatewayProvider(membership),
      callTimeoutMs: 2000,
    }).registerGrain(EchoGrain, { interfaces: [IEcho] });
    await client.connect();
    try {
      // The whole path runs over real sockets: client → gateway → activation, and
      // the reply over a reverse connection back to the client's own listener.
      expect(await client.getGrain(IEcho, "x").echo("over-sockets")).toBe("over-sockets");
    } finally {
      await client.close();
      await silo.stop();
    }
  }, 15_000);
});
