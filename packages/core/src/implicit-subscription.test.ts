import { describe, expect, it } from "vitest";
import { grain, implicitStreamSubscription } from "@tsva/core/decorators";
import { defineGrain } from "@tsva/core/define-grain";
import { Grain } from "@tsva/core/grain";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import {
  implicitStreamObserver,
  STREAM_SUBSCRIPTION_OBSERVER,
  type StreamHandler,
} from "@tsva/core/stream";

describe("implicit stream subscriptions — declaration", () => {
  it("records the namespace on a class grain via the decorator", () => {
    @grain()
    @implicitStreamSubscription("chat")
    class WatcherGrain extends Grain {}

    expect(getGrainMetadata(WatcherGrain)?.implicitSubscriptions).toEqual(["chat"]);
  });

  it("supports several namespaces, regardless of decorator order", () => {
    @implicitStreamSubscription("telemetry")
    @grain()
    @implicitStreamSubscription("chat")
    class MultiGrain extends Grain {}

    expect([...(getGrainMetadata(MultiGrain)?.implicitSubscriptions ?? [])].sort()).toEqual([
      "chat",
      "telemetry",
    ]);
  });

  it("defaults to no implicit subscriptions", () => {
    @grain()
    class PlainGrain extends Grain {}

    expect(getGrainMetadata(PlainGrain)?.implicitSubscriptions).toEqual([]);
  });

  it("records namespaces on a functional grain via defineGrain options", () => {
    const Watcher = defineGrain("FnWatcher", () => ({}), { implicitSubscriptions: ["room"] });
    expect(getGrainMetadata(Watcher)?.implicitSubscriptions).toEqual(["room"]);
  });
});

describe("implicit stream subscriptions — observer", () => {
  it("binds the grain's STREAM_SUBSCRIPTION_OBSERVER member to the instance", async () => {
    const seen: string[] = [];

    @grain()
    @implicitStreamSubscription("chat")
    class WatcherGrain extends Grain {
      tag = "watcher";
      [STREAM_SUBSCRIPTION_OBSERVER](namespace: string, key: string): StreamHandler<unknown> {
        return {
          onNext: async (event) => {
            // Reading `this` proves the observer is bound to the instance.
            seen.push(`${this.tag}:${namespace}/${key}:${String(event)}`);
          },
        };
      }
    }

    const instance = new WatcherGrain();
    const observe = implicitStreamObserver(instance);
    expect(observe).toBeDefined();
    await observe!("chat", "general").onNext("hi", undefined as never);
    expect(seen).toEqual(["watcher:chat/general:hi"]);
  });

  it("returns undefined when the grain declares no observer", () => {
    @grain()
    class PlainGrain extends Grain {}
    expect(implicitStreamObserver(new PlainGrain())).toBeUndefined();
  });
});
