import { describe, expect, it } from "vitest";
import {
  BROADCAST_CHANNEL_OBSERVER,
  type BroadcastChannelHandler,
} from "@thresh/core/broadcast-channel";
import {
  grain,
  implicitChannelSubscription,
  implicitStreamSubscription,
} from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Guid } from "@thresh/core/guid";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@thresh/core/stream";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { implicitSubscriberIds } from "@thresh/streams/implicit-subscriptions";

interface IIntegerWatcher extends GrainKey<bigint> {
  keyAsSeen(): Promise<string>;
}
const IIntegerWatcher = defineGrainInterface<IIntegerWatcher>("IIntegerWatcher.identity", {
  key: "integer",
});

interface IGuidWatcher extends GrainKey<Guid> {
  keyAsSeen(): Promise<string>;
}
const IGuidWatcher = defineGrainInterface<IGuidWatcher>("IGuidWatcher.identity", { key: "guid" });

interface IIntegerChannel extends GrainKey<bigint> {
  keyAsSeen(): Promise<string>;
}
const IIntegerChannel = defineGrainInterface<IIntegerChannel>("IIntegerChannel.identity", {
  key: "integer",
});

interface IUndeclared {
  keyAsSeen(): Promise<string>;
}
const IUndeclared = defineGrainInterface<IUndeclared>("IUndeclared.identity");

const seen: Array<{ kind: string; key: string }> = [];

function describeKey(grainId: GrainId): string {
  const key = grainId.key;
  if (key instanceof Guid) return `guid:${key}`;
  return `${typeof key}:${String(key)}`;
}

@grain()
@implicitStreamSubscription("ticks")
class IntegerWatcherGrain extends Grain implements IIntegerWatcher {
  async keyAsSeen(): Promise<string> {
    return describeKey(this.id);
  }
  [STREAM_SUBSCRIPTION_OBSERVER](): StreamHandler<unknown> {
    return {
      onNext: async () => {
        seen.push({ kind: this.id.keyKind, key: describeKey(this.id) });
      },
    };
  }
}

@grain()
@implicitStreamSubscription("pings")
class GuidWatcherGrain extends Grain implements IGuidWatcher {
  async keyAsSeen(): Promise<string> {
    return describeKey(this.id);
  }
  [STREAM_SUBSCRIPTION_OBSERVER](): StreamHandler<unknown> {
    return {
      onNext: async () => {
        seen.push({ kind: this.id.keyKind, key: describeKey(this.id) });
      },
    };
  }
}

@grain()
@implicitChannelSubscription("alarms")
class IntegerChannelGrain extends Grain implements IIntegerChannel {
  async keyAsSeen(): Promise<string> {
    return describeKey(this.id);
  }
  [BROADCAST_CHANNEL_OBSERVER](): BroadcastChannelHandler<unknown> {
    return {
      onPublished: async () => {
        seen.push({ kind: this.id.keyKind, key: describeKey(this.id) });
      },
    };
  }
}

@grain()
class UndeclaredGrain extends Grain implements IUndeclared {
  async keyAsSeen(): Promise<string> {
    return describeKey(this.id);
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildNode(): ClusterNode {
  const node = new ClusterNode({
    local,
    clusterId: "c1",
    membership: new StaticMembershipService(local, [local]),
    transport: new InProcessTransport(new InProcessNetwork(), "c1"),
  });
  node.registerGrain(IntegerWatcherGrain, { interfaces: [IIntegerWatcher] });
  node.registerGrain(GuidWatcherGrain, { interfaces: [IGuidWatcher] });
  node.registerGrain(IntegerChannelGrain, { interfaces: [IIntegerChannel] });
  node.registerGrain(UndeclaredGrain, { interfaces: [IUndeclared] });
  return node;
}

function implicitTargets(node: ClusterNode, namespace: string, key: string): GrainId[] {
  return implicitSubscriberIds(
    `${namespace}/${key}`,
    (ns) => node.implicitGrainTypes(ns),
    (type) => node.grainKeyKind(type),
  );
}

describe("one grain identity across key kinds", () => {
  it("builds an implicit subscriber's id through the declared key kind", () => {
    const guid = Guid.newGuid();
    const node = buildNode();
    const [integerId] = implicitTargets(node, "ticks", "42");
    const [guidId] = implicitTargets(node, "pings", guid.toString());
    expect(integerId!.key).toBe(42n);
    expect(guidId!.key).toBeInstanceOf(Guid);
    expect(integerId!.equals(new GrainId("IntegerWatcher", 42n))).toBe(true);
  });

  it("activates an integer-keyed grain under its declared kind on an implicit delivery", async () => {
    seen.length = 0;
    const node = buildNode();
    await node.start();
    try {
      const [target] = implicitTargets(node, "ticks", "7");
      await node.deliverStreamEvent(target!, "ticks/7", "tick", 1);
      expect(seen).toEqual([{ kind: "integer", key: "bigint:7" }]);
    } finally {
      await node.stop();
    }
  });

  it("activates a guid-keyed grain under its declared kind on an implicit delivery", async () => {
    seen.length = 0;
    const guid = Guid.newGuid();
    const node = buildNode();
    await node.start();
    try {
      const [target] = implicitTargets(node, "pings", guid.toString());
      await node.deliverStreamEvent(target!, `pings/${guid}`, "ping", 1);
      expect(seen).toEqual([{ kind: "guid", key: `guid:${guid}` }]);
      expect(await node.getGrain(IGuidWatcher, guid).keyAsSeen()).toBe(`guid:${guid}`);
    } finally {
      await node.stop();
    }
  });

  it("gives the declared kind to a grain first reached through a string-built id", async () => {
    seen.length = 0;
    const node = buildNode();
    await node.start();
    try {
      await node.deliverStreamEvent(new GrainId("IntegerWatcher", "9"), "ticks/9", "a", 1);
      expect(await node.getGrain(IIntegerWatcher, 9n).keyAsSeen()).toBe("bigint:9");
      await node.deliverStreamEvent(new GrainId("IntegerWatcher", 9n), "ticks/9", "b", 2);
      expect(seen.map((s) => s.key)).toEqual(["bigint:9", "bigint:9"]);
    } finally {
      await node.stop();
    }
  });

  it("gives the declared kind when the typed reference arrives first", async () => {
    seen.length = 0;
    const node = buildNode();
    await node.start();
    try {
      expect(await node.getGrain(IIntegerWatcher, 11n).keyAsSeen()).toBe("bigint:11");
      await node.deliverStreamEvent(new GrainId("IntegerWatcher", "11"), "ticks/11", "a", 1);
      expect(seen).toEqual([{ kind: "integer", key: "bigint:11" }]);
    } finally {
      await node.stop();
    }
  });

  it("activates an integer-keyed grain under its declared kind on a broadcast-channel delivery", async () => {
    seen.length = 0;
    const node = buildNode();
    await node.start();
    try {
      await node.publishToBroadcastChannel("p", { namespace: "alarms", key: "3" }, "ring");
      expect(seen).toEqual([{ kind: "integer", key: "bigint:3" }]);
    } finally {
      await node.stop();
    }
  });

  it("refuses a key of the wrong kind for a declared interface", async () => {
    const node = buildNode();
    await node.start();
    try {
      expect(() => node.getGrain(IIntegerWatcher, "7" as never)).toThrow(
        /IIntegerWatcher\.identity.*integer.*string/,
      );
      expect(() => node.getGrain(IGuidWatcher, 7n as never)).toThrow(/guid.*integer/);
    } finally {
      await node.stop();
    }
  });

  it("leaves an interface that declares no key kind unchecked", async () => {
    const node = buildNode();
    await node.start();
    try {
      expect(await node.getGrain(IUndeclared, "x" as never).keyAsSeen()).toBe("string:x");
      expect(await node.getGrain(IUndeclared, 5n as never).keyAsSeen()).toBe("bigint:5");
    } finally {
      await node.stop();
    }
  });
});
