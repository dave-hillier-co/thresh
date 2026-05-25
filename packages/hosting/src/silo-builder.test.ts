import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";

interface ICounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.hosting");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildHost() {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMessagePackSerialization()
    .registerGrain(CounterGrain, { interfaces: [ICounter] })
    .build();
}

describe("createSilo / SiloHost", () => {
  it("is not ready until started, ready after, and serves grain calls", async () => {
    const host = buildHost();
    expect(host.health.ready().ok).toBe(false);

    await host.start();
    try {
      expect(host.health.ready().ok).toBe(true);
      expect(await host.getGrain(ICounter, "x").increment(5)).toBe(5);
      expect(host.isActive(new GrainId("Counter", "x"))).toBe(true);
    } finally {
      await host.stop();
    }
  });

  it("flips readiness off when drained", async () => {
    const host = buildHost();
    await host.start();
    expect(host.health.ready().ok).toBe(true);
    await host.stop();
    expect(host.health.ready().ok).toBe(false);
  });

  it("refuses to build without membership or transport", () => {
    expect(() => createSilo({ clusterId: "c1", local }).build()).toThrow(/membership/);
    expect(() =>
      createSilo({ clusterId: "c1", local }).useStaticMembership([local]).build(),
    ).toThrow(/transport/);
  });
});
