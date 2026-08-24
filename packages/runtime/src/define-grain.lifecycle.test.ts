import { beforeEach, describe, expect, it } from "vitest";
import { defineGrain, useOnActivate, useOnDeactivate } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface ILayered extends GrainKey<string> {
  ping(): Promise<string>;
}

const ILayered = defineGrainInterface<ILayered>("ILayered.define-grain-lifecycle");
const IBadActivate = defineGrainInterface<ILayered>("IBadActivate.define-grain-lifecycle");
const IBadDeactivate = defineGrainInterface<ILayered>("IBadDeactivate.define-grain-lifecycle");

// Module-scoped sink the grains write lifecycle events to.
let events: string[] = [];

// Two independent "layers" register their own hooks, as separate helpers would:
// activation runs them in registration order, deactivation unwinds them LIFO.
const LayeredGrain = defineGrain<ILayered>("Layered", (ctx) => {
  useOnActivate((reason) => {
    events.push(`activate:outer:${reason}`);
  });
  useOnDeactivate((reason) => {
    events.push(`deactivate:outer:${reason.code}`);
  });

  useOnActivate(async (reason) => {
    await Promise.resolve();
    events.push(`activate:inner:${reason}`);
  });
  useOnDeactivate(async () => {
    await Promise.resolve();
    events.push("deactivate:inner");
  });

  return {
    ping: async (): Promise<string> => {
      events.push(`ping:${String(ctx.id.key)}`);
      return "pong";
    },
  };
});

// An activate hook that throws fails the activation; a deactivate hook that
// throws must still let the rest of the stack unwind.
const BadActivateGrain = defineGrain<ILayered>("BadActivate", () => {
  useOnActivate(() => {
    events.push("activate:first");
  });
  useOnActivate(() => {
    throw new Error("boom");
  });
  useOnActivate(() => {
    events.push("activate:third");
  });
  return { ping: async (): Promise<string> => "pong" };
});

const BadDeactivateGrain = defineGrain<ILayered>("BadDeactivate", () => {
  useOnDeactivate(() => {
    events.push("deactivate:first-registered");
  });
  useOnDeactivate(() => {
    throw new Error("teardown boom");
  });
  useOnDeactivate(() => {
    events.push("deactivate:last-registered");
  });
  return { ping: async (): Promise<string> => "pong" };
});

const flush = (): Promise<unknown> => new Promise((r) => setTimeout(r, 0));
const layeredId = (key: string): GrainId => new GrainId("Layered", key);

describe("defineGrain lifecycle hooks (sociable)", () => {
  let time: FakeTimeProvider;
  let silo: Silo;

  beforeEach(() => {
    events = [];
    time = new FakeTimeProvider();
    silo = new Silo({ time, defaultCollectionAgeSeconds: 30, collectionIntervalSeconds: 10 });
    silo.registerGrain(LayeredGrain, { interfaces: [ILayered] });
    silo.registerGrain(BadActivateGrain, { interfaces: [IBadActivate] });
    silo.registerGrain(BadDeactivateGrain, { interfaces: [IBadDeactivate] });
    silo.start();
  });

  it("runs every registered activate hook, in registration order, before the first message", async () => {
    const grain = silo.getGrain(ILayered, "a");
    expect(await grain.ping()).toBe("pong");
    expect(events).toEqual([
      "activate:outer:incoming-call",
      "activate:inner:incoming-call",
      "ping:a",
    ]);
  });

  it("unwinds deactivate hooks LIFO, after the activate hooks that set them up", async () => {
    const grain = silo.getGrain(ILayered, "a");
    await grain.ping();

    // Move past the 30s collection age; the 10s collector sweep deactivates it.
    time.advance(31_000);
    await flush();

    expect(silo.isActive(layeredId("a"))).toBe(false);
    expect(events.slice(events.indexOf("ping:a") + 1)).toEqual([
      "deactivate:inner",
      "deactivate:outer:idle",
    ]);
  });

  it("fails the activation when an activate hook throws, skipping the hooks after it", async () => {
    await expect(silo.getGrain(IBadActivate, "a").ping()).rejects.toThrow("boom");
    expect(events).toEqual(["activate:first"]);
  });

  it("keeps unwinding the remaining deactivate hooks when one throws", async () => {
    const grain = silo.getGrain(IBadDeactivate, "a");
    await grain.ping();
    events = [];

    time.advance(31_000);
    await flush();

    // LIFO: last-registered first, then the thrower, then the first-registered
    // one — which must still run rather than being stranded by the throw.
    expect(events).toEqual(["deactivate:last-registered", "deactivate:first-registered"]);
  });

  it("rejects a factory that still returns lifecycle hooks in its surface", () => {
    const Legacy = defineGrain<ILayered>(
      "LegacyLifecycle",
      () =>
        ({
          ping: async (): Promise<string> => "pong",
          onActivate: async (): Promise<void> => undefined,
        }) as unknown as ILayered,
    );

    // The factory runs when the runtime binds the context, so the shadowing
    // surface is caught there rather than silently replacing the composed hooks.
    expect(() => new Legacy.grain().setContext({ id: layeredId("a") } as never)).toThrow(
      /useOnActivate/,
    );
  });
});
