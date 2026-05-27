import { describe, expect, it } from "vitest";
import {
  BROADCAST_CHANNEL_OBSERVER,
  type BroadcastChannelHandler,
  channelKey,
} from "@tsva/core/broadcast-channel";
import { grain, implicitChannelSubscription } from "@tsva/core/decorators";
import { defineGrain } from "@tsva/core/define-grain";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";

// Module sink so observed items survive across activations (keyed by grain key).
let observed: Array<{ key: string; namespace: string; item: unknown }> = [];

interface IWatcher extends GrainWithStringKey {
  marker(): Promise<string>;
}
const IWatcher = defineGrainInterface<IWatcher>("IWatcher.broadcast");

@grain()
@implicitChannelSubscription("alerts")
class WatcherGrain extends Grain implements IWatcher {
  async marker(): Promise<string> {
    return "watcher";
  }
  [BROADCAST_CHANNEL_OBSERVER](namespace: string): BroadcastChannelHandler<unknown> {
    return {
      onPublished: async (item) => {
        observed.push({ key: String(this.id.key), namespace, item });
      },
    };
  }
}

interface IPlain extends GrainWithStringKey {
  marker(): Promise<string>;
}
const IPlain = defineGrainInterface<IPlain>("IPlain.broadcast");

// Implicitly subscribed but declares no observer — the publish must be dropped.
@grain()
@implicitChannelSubscription("noop")
class PlainGrain extends Grain implements IPlain {
  async marker(): Promise<string> {
    return "plain";
  }
}

interface IPublisher extends GrainWithStringKey {
  raise(item: string): Promise<void>;
}
const IPublisher = defineGrainInterface<IPublisher>("IPublisher.broadcast");

// A producer grain publishes to a channel via the broadcast provider.
@grain()
class PublisherGrain extends Grain implements IPublisher {
  async raise(item: string): Promise<void> {
    const provider = this.runtime.getBroadcastChannelProvider();
    const writer = provider.getChannelWriter<string>({ namespace: "alerts", key: "eu" });
    await writer.publish(item);
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildNode(): ClusterNode {
  const network = new InProcessNetwork();
  const node = new ClusterNode({
    local,
    clusterId: "c1",
    membership: new StaticMembershipService(local, [local]),
    transport: new InProcessTransport(network, "c1"),
    broadcastProviders: ["default"],
  });
  node.registerGrain(WatcherGrain, { interfaces: [IWatcher] });
  node.registerGrain(PlainGrain, { interfaces: [IPlain] });
  node.registerGrain(PublisherGrain, { interfaces: [IPublisher] });
  return node;
}

describe("channelKey", () => {
  it("encodes namespace and key as namespace/key", () => {
    expect(channelKey({ namespace: "alerts", key: "eu" })).toBe("alerts/eu");
  });
});

describe("broadcast channels — implicit subscriber registration", () => {
  it("exposes the registered namespace → grain types", () => {
    const node = buildNode();
    expect(node.broadcastGrainTypes("alerts")).toEqual(["Watcher"]);
    expect(node.broadcastGrainTypes("nope")).toEqual([]);
  });
});

describe("broadcast channels — publish fan-out through the dispatcher", () => {
  it("delivers a published item to the matching-key grain with no explicit subscribe", async () => {
    observed = [];
    const node = buildNode();
    await node.start();
    try {
      await node.publishToBroadcastChannel("default", { namespace: "alerts", key: "eu" }, "fire");
      await node.publishToBroadcastChannel("default", { namespace: "alerts", key: "eu" }, "again");
      expect(observed).toEqual([
        { key: "eu", namespace: "alerts", item: "fire" },
        { key: "eu", namespace: "alerts", item: "again" },
      ]);
      // Distinct channel keys reach distinct grain activations.
      await node.publishToBroadcastChannel("default", { namespace: "alerts", key: "us" }, "hi");
      expect(observed.at(-1)).toEqual({ key: "us", namespace: "alerts", item: "hi" });
    } finally {
      await node.stop();
    }
  });

  it("delivers when a producer grain publishes via getBroadcastChannelProvider", async () => {
    observed = [];
    const node = buildNode();
    await node.start();
    try {
      const publisher = node.getGrain(IPublisher, "p1");
      await publisher.raise("evacuate");
      expect(observed).toEqual([{ key: "eu", namespace: "alerts", item: "evacuate" }]);
    } finally {
      await node.stop();
    }
  });

  it("drops a publish for an implicit subscriber that declares no observer", async () => {
    const node = buildNode();
    await node.start();
    try {
      await expect(
        node.publishToBroadcastChannel("default", { namespace: "noop", key: "x" }, "ignored"),
      ).resolves.toBeUndefined();
    } finally {
      await node.stop();
    }
  });
});

// Functional grains declare the same way (defineGrain options + observer member).
const FnWatcher = defineGrain<IWatcher>(
  "FnWatcher",
  (ctx) => ({
    marker: async () => "fn-watcher",
    [BROADCAST_CHANNEL_OBSERVER]: (namespace: string): BroadcastChannelHandler<unknown> => ({
      onPublished: async (item) => {
        observed.push({ key: String(ctx.id.key), namespace, item });
      },
    }),
  }),
  { implicitChannelSubscriptions: ["room"] },
);
const IFnWatcher = defineGrainInterface<IWatcher>("IFnWatcher.broadcast");

describe("broadcast channels — functional grain", () => {
  it("auto-subscribes a functional grain by key", async () => {
    observed = [];
    const network = new InProcessNetwork();
    const node = new ClusterNode({
      local,
      clusterId: "c1",
      membership: new StaticMembershipService(local, [local]),
      transport: new InProcessTransport(network, "c1"),
      broadcastProviders: ["default"],
    });
    node.registerGrain(FnWatcher, { interfaces: [IFnWatcher] });
    expect(node.broadcastGrainTypes("room")).toEqual(["FnWatcher"]);
    await node.start();
    try {
      await node.publishToBroadcastChannel(
        "default",
        { namespace: "room", key: "lobby" },
        "joined",
      );
      expect(observed).toEqual([{ key: "lobby", namespace: "room", item: "joined" }]);
    } finally {
      await node.stop();
    }
  });
});
