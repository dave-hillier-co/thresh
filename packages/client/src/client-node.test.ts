import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { createClient } from "@tsva/client/client-node";

interface ICounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
  fail(): Promise<void>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.client");

interface IUnregistered extends GrainWithStringKey {
  ping(): Promise<void>;
}
const IUnregistered = defineGrainInterface<IUnregistered>("IUnregistered.client");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
  async fail(): Promise<void> {
    throw new Error("boom");
  }
}

const CLUSTER = "c1";
const gatewayAddr = new SiloAddress("gateway", "uid-g", "gateway:11111");
const clientAddr = new SiloAddress("client", "uid-c", "client:22222");

function startGateway(network: InProcessNetwork): ClusterNode {
  const gateway = new ClusterNode({
    local: gatewayAddr,
    clusterId: CLUSTER,
    membership: new StaticMembershipService(gatewayAddr, [gatewayAddr]),
    transport: new InProcessTransport(network, CLUSTER),
    random: () => 0,
  });
  gateway.registerGrain(CounterGrain, { interfaces: [ICounter] });
  return gateway;
}

describe("external client", () => {
  it("routes getGrain calls through the gateway to a single activation", async () => {
    const network = new InProcessNetwork();
    const gateway = startGateway(network);
    await gateway.start();
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: gatewayAddr,
    }).registerGrain(CounterGrain, { interfaces: [ICounter] });
    await client.connect();
    try {
      const counter = client.getGrain(ICounter, "x");
      expect(await counter.increment(5)).toBe(5);
      expect(await counter.increment(3)).toBe(8); // same activation, state shared
      expect(gateway.isActive(new GrainId("Counter", "x"))).toBe(true);
    } finally {
      await client.close();
      await gateway.stop();
    }
  });

  it("propagates an application error thrown by the grain", async () => {
    const network = new InProcessNetwork();
    const gateway = startGateway(network);
    await gateway.start();
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: gatewayAddr,
    }).registerGrain(CounterGrain, { interfaces: [ICounter] });
    await client.connect();
    try {
      await expect(client.getGrain(ICounter, "y").fail()).rejects.toThrow("boom");
    } finally {
      await client.close();
      await gateway.stop();
    }
  });

  it("rejects getGrain for an interface the client did not register", async () => {
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(new InProcessNetwork(), CLUSTER),
      gateway: gatewayAddr,
    });
    expect(() => client.getGrain(IUnregistered, "z")).toThrow(/no grain registered/);
  });
});
