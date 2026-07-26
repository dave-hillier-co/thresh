import { describe, expect, it } from "vitest";
import { StreamProducerRegistry } from "@thresh/streams/stream-producer-registry";

describe("StreamProducerRegistry", () => {
  it("counts a registered producer until it unregisters", async () => {
    const registry = new StreamProducerRegistry();
    expect(registry.producerCount("chat", "room-1")).toBe(0);

    const handle = registry.register("default", "chat", "room-1");
    expect(registry.producerCount("chat", "room-1")).toBe(1);
    expect(handle.streamId).toEqual({ provider: "default", namespace: "chat", key: "room-1" });

    await handle.unregister();
    expect(registry.producerCount("chat", "room-1")).toBe(0);
  });

  it("tracks multiple producers on the same stream independently", async () => {
    const registry = new StreamProducerRegistry();
    const a = registry.register("default", "chat", "room-1");
    const b = registry.register("default", "chat", "room-1");
    expect(registry.producerCount("chat", "room-1")).toBe(2);

    await a.unregister();
    expect(registry.producerCount("chat", "room-1")).toBe(1);
    await b.unregister();
    expect(registry.producerCount("chat", "room-1")).toBe(0);
  });

  it("is idempotent: unregistering twice does not double-decrement", async () => {
    const registry = new StreamProducerRegistry();
    const handle = registry.register("default", "chat", "room-1");
    await handle.unregister();
    await handle.unregister();
    expect(registry.producerCount("chat", "room-1")).toBe(0);
  });

  it("keeps producers on different streams independent", () => {
    const registry = new StreamProducerRegistry();
    registry.register("default", "chat", "room-1");
    expect(registry.producerCount("chat", "room-2")).toBe(0);
  });
});
