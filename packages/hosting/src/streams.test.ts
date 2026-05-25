import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import type { StreamHandler } from "@tsva/core/stream";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";

interface IDevice extends GrainWithStringKey {
  report(reading: number): Promise<void>;
}
const IDevice = defineGrainInterface<IDevice>("IDevice.stream");

interface IAggregator extends GrainWithStringKey {
  readings(): Promise<number[]>;
}
const IAggregator = defineGrainInterface<IAggregator>("IAggregator.stream");

@grain()
class DeviceGrain extends Grain implements IDevice {
  async report(reading: number): Promise<void> {
    await this.runtime
      .getStreamProvider()
      .getStream<number>("telemetry", this.id.key)
      .publish(reading);
  }
}

@grain()
class AggregatorGrain extends Grain implements IAggregator {
  private received: number[] = [];

  override async onActivate(): Promise<void> {
    const stream = this.runtime.getStreamProvider().getStream<number>("telemetry", this.id.key);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0]!.resume(this.handler());
    else await stream.subscribe(this.handler());
  }

  async readings(): Promise<number[]> {
    return this.received;
  }

  private handler(): StreamHandler<number> {
    // onNext mutates grain state with no lock — proof it runs as a turn on this activation.
    return { onNext: async (reading) => void this.received.push(reading) };
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("streams end-to-end", () => {
  it("delivers published events to a subscribing grain, in order, as turns", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMemoryStreams()
      .registerGrain(DeviceGrain, { interfaces: [IDevice] })
      .registerGrain(AggregatorGrain, { interfaces: [IAggregator] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IAggregator, "dev-1").readings(); // activate -> subscribe in onActivate
      await silo.getGrain(IDevice, "dev-1").report(10);
      await silo.getGrain(IDevice, "dev-1").report(20);
      await flush();
      expect(await silo.getGrain(IAggregator, "dev-1").readings()).toEqual([10, 20]);
    } finally {
      await silo.stop();
    }
  });

  it("throws if a grain uses streams on a silo without them configured", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(DeviceGrain, { interfaces: [IDevice] })
      .build();
    await silo.start();
    try {
      await expect(silo.getGrain(IDevice, "dev-1").report(1)).rejects.toThrow(/streams/);
    } finally {
      await silo.stop();
    }
  });
});
