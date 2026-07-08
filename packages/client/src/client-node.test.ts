import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { GrainCancellationToken } from "@tsva/core/grain-cancellation-token";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { staticGatewayProvider } from "@tsva/client/gateway-provider";
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

interface IWaiter extends GrainWithStringKey {
  wait(token: GrainCancellationToken): Promise<void>;
}
const IWaiter = defineGrainInterface<IWaiter>("IWaiter.client");

@grain()
class WaiterGrain extends Grain implements IWaiter {
  async wait(token: GrainCancellationToken): Promise<void> {
    if (token.isCancellationRequested) throw new Error("cancelled");
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, 10_000);
      token.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          reject(new Error("cancelled"));
        },
        { once: true },
      );
    });
  }
}

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

  it("backs off exponentially between failed gateway attempts rather than busy-spinning", async () => {
    const network = new InProcessNetwork();
    // Three gateways that nobody is listening on — every connect fails.
    const dead = [
      new SiloAddress("dead-1", "uid-d1", "dead-1:1"),
      new SiloAddress("dead-2", "uid-d2", "dead-2:2"),
      new SiloAddress("dead-3", "uid-d3", "dead-3:3"),
    ];
    const delays: number[] = [];
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateways: staticGatewayProvider(dead),
      delay: async (ms: number) => {
        delays.push(ms);
      },
    }).registerGrain(CounterGrain, { interfaces: [ICounter] });
    await client.connect();
    try {
      await expect(client.getGrain(ICounter, "x").increment(1)).rejects.toThrow();
    } finally {
      await client.close();
    }
    // We expect a backoff between each failed attempt, starting at 100ms and
    // doubling, capped at 2000ms (Orleans MINIMUM_INTERCONNECT_DELAY = 100ms).
    expect(delays.length).toBeGreaterThanOrEqual(3);
    expect(delays[0]).toBe(100);
    expect(delays[1]).toBe(200);
    expect(delays[2]).toBe(400);
    for (const d of delays) expect(d).toBeLessThanOrEqual(2000);
  });

  it("cancels a grain call via a client-created GrainCancellationTokenSource", async () => {
    const network = new InProcessNetwork();
    const gateway = new ClusterNode({
      local: gatewayAddr,
      clusterId: CLUSTER,
      membership: new StaticMembershipService(gatewayAddr, [gatewayAddr]),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0,
    });
    gateway.registerGrain(WaiterGrain, { interfaces: [IWaiter] });
    await gateway.start();
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: gatewayAddr,
    }).registerGrain(WaiterGrain, { interfaces: [IWaiter] });
    await client.connect();
    try {
      // Recording happens on the client's own outgoing dispatch, before this
      // call ever crosses the wire — so the source knows to notify the grain
      // even though the client (unlike a silo) always serializes the call.
      const cts = client.newCancellationTokenSource();
      const task = client.getGrain(IWaiter, "w").wait(cts.token);
      await cts.cancel();
      await expect(task).rejects.toThrow(/cancelled/);
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
