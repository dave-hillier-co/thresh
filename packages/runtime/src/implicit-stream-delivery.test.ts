import { describe, expect, it } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { defineGrain } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@thresh/core/stream";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

// Module sink so observed events survive across activations (keyed by grain key).
let observed: Array<{ key: string; namespace: string; event: unknown }> = [];

interface IWatcher extends GrainKey<string> {
  marker(): Promise<string>;
}
const IWatcher = defineGrainInterface<IWatcher>("IWatcher.implicit");

@grain()
@implicitStreamSubscription("chat")
class WatcherGrain extends Grain implements IWatcher {
  async marker(): Promise<string> {
    return "watcher";
  }
  [STREAM_SUBSCRIPTION_OBSERVER](namespace: string, _key: string): StreamHandler<unknown> {
    return {
      onNext: async (event) => {
        observed.push({ key: String(this.id.key), namespace, event });
      },
    };
  }
}

interface IPlain extends GrainKey<string> {
  marker(): Promise<string>;
}
const IPlain = defineGrainInterface<IPlain>("IPlain.implicit");

// Implicitly subscribed but declares no observer — the event must be dropped.
@grain()
@implicitStreamSubscription("noop")
class PlainGrain extends Grain implements IPlain {
  async marker(): Promise<string> {
    return "plain";
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
  });
  node.registerGrain(WatcherGrain, { interfaces: [IWatcher] });
  node.registerGrain(PlainGrain, { interfaces: [IPlain] });
  return node;
}

describe("implicit stream subscriptions — delivery through the dispatcher", () => {
  it("exposes the registered namespace → grain types", () => {
    const node = buildNode();
    expect(node.implicitGrainTypes("chat")).toEqual(["Watcher"]);
    expect(node.implicitGrainTypes("nope")).toEqual([]);
  });

  it("delivers a stream event to the matching-key grain with no explicit subscribe", async () => {
    observed = [];
    const node = buildNode();
    await node.start();
    try {
      // The fan-out would address (Watcher, "general") for stream chat/general.
      const watcher = new GrainId("Watcher", "general");
      await node.deliverStreamEvent(watcher, "chat/general", "hello", 1);
      await node.deliverStreamEvent(watcher, "chat/general", "world", 2);

      expect(observed).toEqual([
        { key: "general", namespace: "chat", event: "hello" },
        { key: "general", namespace: "chat", event: "world" },
      ]);
      // Distinct stream keys reach distinct grain activations.
      await node.deliverStreamEvent(new GrainId("Watcher", "random"), "chat/random", "hi", 1);
      expect(observed.at(-1)).toEqual({ key: "random", namespace: "chat", event: "hi" });
    } finally {
      await node.stop();
    }
  });

  it("drops an event for an implicit subscriber that declares no observer", async () => {
    const node = buildNode();
    await node.start();
    try {
      await expect(
        node.deliverStreamEvent(new GrainId("Plain", "x"), "noop/x", "ignored", 1),
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
    [STREAM_SUBSCRIPTION_OBSERVER]: (namespace: string, _key: string): StreamHandler<unknown> => ({
      onNext: async (event) => {
        observed.push({ key: String(ctx.id.key), namespace, event });
      },
    }),
  }),
  { implicitSubscriptions: ["room"] },
);
const IFnWatcher = defineGrainInterface<IWatcher>("IFnWatcher.implicit");

describe("implicit stream subscriptions — functional grain", () => {
  it("auto-subscribes a functional grain by key", async () => {
    observed = [];
    const network = new InProcessNetwork();
    const node = new ClusterNode({
      local,
      clusterId: "c1",
      membership: new StaticMembershipService(local, [local]),
      transport: new InProcessTransport(network, "c1"),
    });
    node.registerGrain(FnWatcher, { interfaces: [IFnWatcher] });
    expect(node.implicitGrainTypes("room")).toEqual(["FnWatcher"]);
    await node.start();
    try {
      await node.deliverStreamEvent(new GrainId("FnWatcher", "lobby"), "room/lobby", "joined", 1);
      expect(observed).toEqual([{ key: "lobby", namespace: "room", event: "joined" }]);
    } finally {
      await node.stop();
    }
  });
});
