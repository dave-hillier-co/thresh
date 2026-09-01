import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";

interface IEventObserver extends GrainKey<string> {
  onEvent(text: string): Promise<string>;
}
const IEventObserver = defineGrainInterface<IEventObserver>("test.IEventObserver");

interface ISubscriberGrain extends GrainKey<string> {
  subscribe(observer: IEventObserver): Promise<void>;
  poke(text: string): Promise<string>;
}
const ISubscriberGrain = defineGrainInterface<ISubscriberGrain>("test.ISubscriberGrain");

@grain()
class SubscriberGrain extends Grain implements ISubscriberGrain {
  private observer: IEventObserver | undefined;

  async subscribe(observer: IEventObserver): Promise<void> {
    this.observer = observer;
  }

  async poke(text: string): Promise<string> {
    if (this.observer === undefined) throw new Error("no observer subscribed");
    return this.observer.onEvent(text);
  }
}

const CLUSTER = "c1";

function buildSilo(
  local: SiloAddress,
  network: InProcessNetwork,
  membershipAddrs: SiloAddress[],
  random: () => number = () => 0,
) {
  const silo = new ClusterNode({
    local,
    clusterId: CLUSTER,
    membership: new StaticMembershipService(local, membershipAddrs),
    transport: new InProcessTransport(network, CLUSTER),
    random,
  });
  silo.registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
  return silo;
}

describe("grain -> observer callback routing (dispatcher client-locator + deliver-to-proxy)", () => {
  it("routes a grain call to a client-hosted observer on the same-silo gateway and returns its result", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr]);
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
    await client.connect();

    try {
      const received: string[] = [];
      const ref = client.createObjectReference(IEventObserver, {
        onEvent: async (text: string) => {
          received.push(text);
          return `ack:${text}`;
        },
      });

      const grainRef = client.getGrain(ISubscriberGrain, "sub-1");
      await grainRef.subscribe(ref);
      const result = await grainRef.poke("hello");

      expect(received).toEqual(["hello"]);
      expect(result).toBe("ack:hello");
    } finally {
      await client.close();
      await silo.stop();
    }
  });

  it("resolves a client's invoke() response via the held accepted connection, even though the client's identity address is undialable (issue #65)", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr]);
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
    await client.connect();

    try {
      // Nothing is registered at the client's own synthetic identity endpoint
      // — dialling it must fail — yet an ordinary call still gets its reply,
      // because `ClusterNode.reply` answers down the held accepted
      // connection instead of dialling `sendingSilo` back.
      expect(network.lookup(new SiloAddress("x", "x", "client:no-such-client"))).toBeUndefined();
      const grainRef = client.getGrain(ISubscriberGrain, "sub-undialable");
      const ref = client.createObjectReference(IEventObserver, {
        onEvent: async (text: string) => `ack:${text}`,
      });
      await grainRef.subscribe(ref);
      expect(await grainRef.poke("ping")).toBe("ack:ping");
    } finally {
      await client.close();
      await silo.stop();
    }
  });

  it("routes through a gateway silo other than the one hosting the subscriber grain", async () => {
    const network = new InProcessNetwork();
    const silo1Addr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo2Addr = new SiloAddress("silo-2", "uid-2", "silo-2:11112");
    const addrs = [silo1Addr, silo2Addr];
    const silo1 = buildSilo(silo1Addr, network, addrs);
    // Bias placement to the second candidate (silo2) so the subscriber grain
    // activates away from the client's gateway (silo1), forcing the observer
    // call to hop silo2 -> silo1 (gateway) -> client.
    const silo2 = buildSilo(silo2Addr, network, addrs, () => 0.99);
    await silo1.start();
    await silo2.start();

    // Client connects to silo1 as its gateway.
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: silo1Addr,
    }).registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
    await client.connect();

    try {
      const received: string[] = [];
      const ref = client.createObjectReference(IEventObserver, {
        onEvent: async (text: string) => {
          received.push(text);
          return `ack:${text}`;
        },
      });

      // Call through silo2 directly (not the client's gateway) so the
      // subscriber grain activates there, forcing the call to the observer
      // to hop silo2 -> silo1 (gateway) -> client and the reply to relay back.
      const grainOnSilo2 = silo2.getGrain(ISubscriberGrain, "sub-2");
      await grainOnSilo2.subscribe(ref);
      const result = await grainOnSilo2.poke("cross-silo");

      expect(received).toEqual(["cross-silo"]);
      expect(result).toBe("ack:cross-silo");
    } finally {
      await client.close();
      await silo1.stop();
      await silo2.stop();
    }
  });

  it("propagates an error thrown by the hosted observer back to the calling grain", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr]);
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
    await client.connect();

    try {
      const ref = client.createObjectReference(IEventObserver, {
        onEvent: async () => {
          throw new Error("observer boom");
        },
      });

      const grainRef = client.getGrain(ISubscriberGrain, "sub-3");
      await grainRef.subscribe(ref);
      await expect(grainRef.poke("x")).rejects.toThrow("observer boom");
    } finally {
      await client.close();
      await silo.stop();
    }
  });
});
