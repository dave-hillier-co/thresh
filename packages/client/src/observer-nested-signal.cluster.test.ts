import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";

/**
 * The client leg of the nested-signal contract (`cluster.abort-signal-argument.test.ts` covers the
 * silo leg). A silo grain calling a client-hosted observer crosses the wire in the same way, so
 * `ClientNode.bindCancellationTokens` owes the hosted object the same unwrap at the same depth:
 * without it the object's method receives a `CancellationTokenPlaceholder` inside its request
 * record where its signature declares an `AbortSignal`.
 */
interface NotifyRequest {
  readonly text: string;
  readonly signal?: AbortSignal;
}

interface IEventObserver extends GrainWithStringKey {
  onEvent(request: NotifyRequest): Promise<string>;
}
const IEventObserver = defineGrainInterface<IEventObserver>("test.IEventObserver.nestedSignal");

interface ISubscriberGrain extends GrainWithStringKey {
  subscribe(observer: IEventObserver): Promise<void>;
  /** Notify the observer, with the grain's own signal riding INSIDE the request record. */
  poke(text: string): Promise<string>;
}
const ISubscriberGrain = defineGrainInterface<ISubscriberGrain>(
  "test.ISubscriberGrain.nestedSignal",
);

@grain()
class SubscriberGrain extends Grain implements ISubscriberGrain {
  private observer: IEventObserver | undefined;

  async subscribe(observer: IEventObserver): Promise<void> {
    this.observer = observer;
  }

  async poke(text: string): Promise<string> {
    if (this.observer === undefined) throw new Error("no observer subscribed");
    const controller = new AbortController();
    return this.observer.onEvent({ text, signal: controller.signal });
  }
}

const CLUSTER = "nested-signal-observer";

describe("an AbortSignal nested inside an argument to a client-hosted observer", () => {
  it("arrives at the hosted object as a real AbortSignal", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const clientAddr = new SiloAddress("client", "uid-c", "client:22222");
    const silo = new ClusterNode({
      local: siloAddr,
      clusterId: CLUSTER,
      membership: new StaticMembershipService(siloAddr, [siloAddr]),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0,
    });
    silo.registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(SubscriberGrain, { interfaces: [ISubscriberGrain] });
    await client.connect();

    try {
      const ref = client.createObjectReference(IEventObserver, {
        onEvent: async (request: NotifyRequest) => {
          const isSignal = request.signal instanceof AbortSignal;
          return `${request.text}:${isSignal ? "signal" : typeof request.signal}`;
        },
      });

      const grainRef = client.getGrain(ISubscriberGrain, "sub-nested");
      await grainRef.subscribe(ref);

      expect(await grainRef.poke("hello")).toBe("hello:signal");
    } finally {
      await client.close();
      await silo.stop();
    }
  });
});
