import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GrainCallTimeoutError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { createClient } from "@thresh/client/client-node";

interface IGated extends GrainKey<string> {
  run(): Promise<number>;
}
const IGated = defineGrainInterface<IGated>("IGated.clientTimeToLive");

let runs = 0;
let notifyStarted!: () => void;
let started!: Promise<void>;
let release!: () => void;
let gate!: Promise<void>;

@grain()
class GatedGrain extends Grain implements IGated {
  async run(): Promise<number> {
    runs += 1;
    notifyStarted();
    await gate;
    return runs;
  }
}

const CLUSTER = "c1";
const gatewayAddr = new SiloAddress("gateway", "uid-g", "gateway:11111");

// Orleans' client stamps each request with a `TimeToLive` of its response
// timeout (`OutsideRuntimeClient.SendRequest`), so the gateway drops a request
// still queued when the client has already stopped waiting for it, rather
// than running it anyway (issue #90).
describe("a client request carries its call timeout as a time-to-live (issue #90)", () => {
  it("is not run by the gateway once the client's call timeout has passed", async () => {
    runs = 0;
    started = new Promise((resolve) => (notifyStarted = resolve));
    gate = new Promise((resolve) => (release = resolve));
    const network = new InProcessNetwork();
    const time = new FakeTimeProvider();
    const gateway = new ClusterNode({
      local: gatewayAddr,
      clusterId: CLUSTER,
      membership: new StaticMembershipService(gatewayAddr, [gatewayAddr]),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0,
      time,
    });
    gateway.registerGrain(GatedGrain, { interfaces: [IGated] });
    await gateway.start();
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: gatewayAddr,
      callTimeoutMs: 5_000,
    }).registerGrain(GatedGrain, { interfaces: [IGated] });
    await client.connect();

    try {
      const grainRef = client.getGrain(IGated, "queued");
      const first = grainRef.run();
      await started;
      const second = grainRef.run().catch((e: unknown) => e);
      // Let the second request reach the gateway and queue behind the first.
      for (let i = 0; i < 5; i++) await new Promise((resolve) => setTimeout(resolve, 0));

      time.advance(6_000); // past the client's 5s call timeout, on the gateway's clock
      release();

      expect(await first).toBe(1);
      expect(await second).toBeInstanceOf(GrainCallTimeoutError);
      expect(runs).toBe(1);
    } finally {
      await client.close();
      await gateway.stop();
    }
  });
});
