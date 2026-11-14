import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { RejectionError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { didNotExecute } from "@thresh/messaging/resend-policy";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";

/**
 * W6: a client-hosted observer whose own nested call is refused, and that lets the refusal
 * escape, has run. Its reply is an error, not a "refused before execution" rejection, and the
 * grain that called it (and that grain's caller) do not resend.
 */
interface IRelayObserver extends GrainKey<string> {
  relay(onward: IRelayObserver): Promise<string>;
}
const IRelayObserver = defineGrainInterface<IRelayObserver>("test.IRelayObserver.nested-refusal");

interface IPoker extends GrainKey<string> {
  subscribe(observer: IRelayObserver, onward: IRelayObserver): Promise<void>;
  poke(): Promise<boolean>;
}
const IPoker = defineGrainInterface<IPoker>("test.IPoker.nested-refusal");

let pokes = 0;

@grain()
class PokerGrain extends Grain implements IPoker {
  private observer: IRelayObserver | undefined;
  private onward: IRelayObserver | undefined;

  async subscribe(observer: IRelayObserver, onward: IRelayObserver): Promise<void> {
    this.observer = observer;
    this.onward = onward;
  }

  /** Calls the observer, and reports whether its failure claimed the observer never ran. */
  async poke(): Promise<boolean> {
    pokes += 1;
    try {
      await this.observer!.relay(this.onward!);
      return false;
    } catch (err) {
      if (!(err instanceof RejectionError)) throw err;
      return didNotExecute(err);
    }
  }
}

const CLUSTER = "c1";

describe("an observer that rethrows its nested call's refusal (W6)", () => {
  it("replies with an error, so its caller does not treat it as refused before execution", async () => {
    pokes = 0;
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = new ClusterNode({
      local: siloAddr,
      clusterId: CLUSTER,
      membership: new StaticMembershipService(siloAddr, [siloAddr]),
      transport: new InProcessTransport(network, CLUSTER),
    });
    silo.registerGrain(PokerGrain, { interfaces: [IPoker] });
    await silo.start();
    const connect = async () => {
      const c = createClient({
        clusterId: CLUSTER,
        transport: new InProcessTransport(network, CLUSTER),
        gateway: siloAddr,
      }).registerGrain(PokerGrain, { interfaces: [IPoker] });
      await c.connect();
      return c;
    };
    // An observer whose client has gone: every call to it is refused before it runs.
    const gone = await connect();
    const goneRef = gone.createObjectReference(IRelayObserver, {
      relay: async () => "never",
    });
    await gone.close();

    const client = await connect();
    let relays = 0;
    try {
      const ref = client.createObjectReference(IRelayObserver, {
        relay: async (onward: IRelayObserver) => {
          relays += 1;
          // The nested call's refusal escapes as it is.
          return onward.relay(onward);
        },
      });
      const poker = client.getGrain(IPoker, "p");
      await poker.subscribe(ref, goneRef);

      expect(await poker.poke()).toBe(false);
      expect(relays).toBe(1);
      expect(pokes).toBe(1);
    } finally {
      await client.close();
      await silo.stop();
    }
  });
});
