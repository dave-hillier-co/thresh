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

/**
 * Issue #89: a call to a client-hosted observer that is already on the wire
 * when the client's gateway socket drops must fail fast, not wait out the full
 * call timeout — the socket it went out on can never carry the reply. A call
 * made after the drop must not be written to the dead socket either.
 */

interface IHangingObserver extends GrainKey<string> {
  onEvent(text: string): Promise<string>;
}
const IHangingObserver = defineGrainInterface<IHangingObserver>("test.IHangingObserver");

interface INotifierGrain extends GrainKey<string> {
  subscribe(observer: IHangingObserver): Promise<void>;
  poke(text: string): Promise<string>;
}
const INotifierGrain = defineGrainInterface<INotifierGrain>("test.INotifierGrain");

@grain()
class NotifierGrain extends Grain implements INotifierGrain {
  private observer: IHangingObserver | undefined;

  async subscribe(observer: IHangingObserver): Promise<void> {
    this.observer = observer;
  }

  async poke(text: string): Promise<string> {
    if (this.observer === undefined) throw new Error("no observer subscribed");
    return this.observer.onEvent(text);
  }
}

const CLUSTER = "c1";
// Far longer than the test's own timeout: a call that is only released by its
// deadline makes the test time out instead of passing.
const CALL_TIMEOUT_MS = 120_000;

function buildSilo(local: SiloAddress, network: InProcessNetwork, addrs: SiloAddress[]) {
  const silo = new ClusterNode({
    local,
    clusterId: CLUSTER,
    membership: new StaticMembershipService(local, addrs),
    transport: new InProcessTransport(network, CLUSTER),
    callTimeoutMs: CALL_TIMEOUT_MS,
    random: () => 0.99,
  });
  silo.registerGrain(NotifierGrain, { interfaces: [INotifierGrain] });
  return silo;
}

async function connectHangingClient(network: InProcessNetwork, gateway: SiloAddress) {
  const client = createClient({
    clusterId: CLUSTER,
    transport: new InProcessTransport(network, CLUSTER),
    gateway,
  }).registerGrain(NotifierGrain, { interfaces: [INotifierGrain] });
  await client.connect();
  let reached!: () => void;
  const observerReached = new Promise<void>((resolve) => (reached = resolve));
  const ref = client.createObjectReference(IHangingObserver, {
    // Never answers: only the gateway noticing the dropped socket can end the call.
    onEvent: () => {
      reached();
      return new Promise<string>(() => undefined);
    },
  });
  return { client, ref, observerReached };
}

describe("a dropped gateway client connection fails its calls fast (issue #89)", () => {
  it("rejects a call already in flight to the client on its own gateway, and a later one", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr]);
    await silo.start();
    const { client, ref, observerReached } = await connectHangingClient(network, siloAddr);

    try {
      // Subscribed through the client so the reference is serialized and the
      // grain holds a silo-side proxy to the observer, as in a real deployment.
      await client.getGrain(INotifierGrain, "n-local").subscribe(ref);
      const grainRef = silo.getGrain(INotifierGrain, "n-local");
      const inFlight = grainRef.poke("in-flight");
      await observerReached;
      await client.close();

      await expect(inFlight).rejects.toThrow(/lost|not connected/);
      await expect(grainRef.poke("after")).rejects.toThrow(/not connected/);
    } finally {
      await silo.stop();
    }
  }, 5_000);

  it("rejects a call already in flight that a peer silo proxied through the client's gateway", async () => {
    const network = new InProcessNetwork();
    const gatewayAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const peerAddr = new SiloAddress("silo-2", "uid-2", "silo-2:11112");
    const addrs = [gatewayAddr, peerAddr];
    const gateway = buildSilo(gatewayAddr, network, addrs);
    const peer = buildSilo(peerAddr, network, addrs);
    await gateway.start();
    await peer.start();
    const { client, ref, observerReached } = await connectHangingClient(network, gatewayAddr);

    try {
      // Called through the peer (placement biased to it), so the observer call
      // hops peer -> gateway -> client and the gateway's proxy leg is the one
      // left waiting on the dead socket.
      await client.getGrain(INotifierGrain, "n-proxied").subscribe(ref);
      const grainRef = peer.getGrain(INotifierGrain, "n-proxied");
      const inFlight = grainRef.poke("in-flight");
      await observerReached;

      await client.close();

      await expect(inFlight).rejects.toThrow(/lost|not connected/);
    } finally {
      await gateway.stop();
      await peer.stop();
    }
  }, 5_000);
});
