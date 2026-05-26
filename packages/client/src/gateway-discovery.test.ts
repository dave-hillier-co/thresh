import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { createClient } from "@tsva/client/client-node";
import {
  membershipGatewayProvider,
  staticGatewayProvider,
} from "@tsva/client/gateway-provider";

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

