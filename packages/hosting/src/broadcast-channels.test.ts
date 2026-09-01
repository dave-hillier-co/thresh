import { describe, expect, it } from "vitest";
import {
  BROADCAST_CHANNEL_OBSERVER,
  type BroadcastChannelHandler,
} from "@thresh/core/broadcast-channel";
import { grain, implicitChannelSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

interface IRaiser extends GrainKey<string> {
  raise(message: string): Promise<void>;
}
const IRaiser = defineGrainInterface<IRaiser>("IRaiser.broadcast");

interface IMonitor extends GrainKey<string> {
  seen(): Promise<string[]>;
}
const IMonitor = defineGrainInterface<IMonitor>("IMonitor.broadcast");

// Publishes to the "alerts" channel keyed by its own grain key.
@grain()
class RaiserGrain extends Grain implements IRaiser {
  async raise(message: string): Promise<void> {
    await this.runtime
      .getBroadcastChannelProvider()
      .getChannelWriter<string>({ namespace: "alerts", key: String(this.id.key) })
      .publish(message);
  }
}

// Implicitly subscribed to "alerts": receives every item on (alerts, its key).
@grain()
@implicitChannelSubscription("alerts")
class MonitorGrain extends Grain implements IMonitor {
  private received: string[] = [];
  async seen(): Promise<string[]> {
    return this.received;
  }
  [BROADCAST_CHANNEL_OBSERVER](): BroadcastChannelHandler<unknown> {
    // onPublished mutates grain state with no lock — proof it runs as a turn here.
    return { onPublished: (item) => void this.received.push(item as string) };
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

describe("broadcast channels end-to-end", () => {
  it("fans a published item out to the implicitly-subscribed grain as a turn", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useBroadcastChannels()
      .registerGrain(RaiserGrain, { interfaces: [IRaiser] })
      .registerGrain(MonitorGrain, { interfaces: [IMonitor] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IRaiser, "zone-1").raise("flood");
      await silo.getGrain(IRaiser, "zone-1").raise("storm");
      // Distinct key is a distinct channel, so zone-2's monitor sees nothing.
      await silo.getGrain(IRaiser, "zone-2").raise("quiet");
      expect(await silo.getGrain(IMonitor, "zone-1").seen()).toEqual(["flood", "storm"]);
      expect(await silo.getGrain(IMonitor, "zone-2").seen()).toEqual(["quiet"]);
    } finally {
      await silo.stop();
    }
  });

  it("throws if a grain uses broadcast channels on a silo without them configured", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(RaiserGrain, { interfaces: [IRaiser] })
      .build();
    await silo.start();
    try {
      await expect(silo.getGrain(IRaiser, "zone-1").raise("x")).rejects.toThrow(/broadcast/);
    } finally {
      await silo.stop();
    }
  });
});
