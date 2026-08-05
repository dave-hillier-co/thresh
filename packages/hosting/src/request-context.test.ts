import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { requestContext } from "@thresh/runtime/invocation-context";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createSilo } from "@thresh/hosting/silo-builder";

interface Downstream extends GrainKey<string> {
  readTenant(): Promise<string | undefined>;
}
interface Caller extends GrainKey<string> {
  propagate(tenant: string, downstream: string): Promise<string | undefined>;
}

const Downstream = defineGrainInterface<Downstream>("RCDownstream");
const Caller = defineGrainInterface<Caller>("RCCaller");

@grain({ name: "RCDownstream", placement: "preferLocal" })
class DownstreamGrain extends Grain implements Downstream {
  async readTenant(): Promise<string | undefined> {
    return requestContext.get("tenant");
  }
}

@grain({ name: "RCCaller", placement: "preferLocal" })
class CallerGrain extends Grain implements Caller {
  async propagate(tenant: string, downstream: string): Promise<string | undefined> {
    requestContext.set("tenant", tenant);
    // The header set here must reach the downstream grain on the call chain.
    return this.getGrain(Downstream, downstream).readTenant();
  }
}

const addrs = [0, 1].map((n) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`));

describe("ambient request context", () => {
  it("propagates a header from a grain to a downstream grain in-process", async () => {
    const silo = createSilo({ clusterId: "rc", local: addrs[0]! })
      .useStaticMembership([addrs[0]!])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(CallerGrain, { interfaces: [Caller] })
      .registerGrain(DownstreamGrain, { interfaces: [Downstream] })
      .build();
    await silo.start();
    try {
      expect(await silo.getGrain(Caller, "c").propagate("acme", "d")).toBe("acme");
      // No ambient context outside a turn / a fresh chain: the header does not leak.
      expect(await silo.getGrain(Downstream, "d2").readTenant()).toBeNull();
    } finally {
      await silo.stop();
    }
  });

  it("propagates a header across silos", async () => {
    const net = new InProcessNetwork();
    const membership = new StaticMembershipService(addrs[0]!, addrs);
    const silos = addrs.map((local) =>
      createSilo({ clusterId: "rc-cluster", local })
        .useMembership(membership)
        .useInProcessTransport(net)
        .registerGrain(CallerGrain, { interfaces: [Caller] })
        .registerGrain(DownstreamGrain, { interfaces: [Downstream] })
        .build(),
    );
    for (const s of silos) await s.start();
    try {
      // Place the downstream grain on silo-1 (preferLocal first-touch from silo-1).
      await silos[1]!.getGrain(Downstream, "d").readTenant();
      // The caller runs on silo-0 and calls the downstream on silo-1: the header
      // must ride the message envelope across the wire.
      expect(await silos[0]!.getGrain(Caller, "c").propagate("acme", "d")).toBe("acme");
    } finally {
      await Promise.all(silos.map((s) => s.stop()));
    }
  });
});
