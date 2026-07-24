import { describe, expect, it } from "vitest";
import { GeneratorPullingStreamProvider } from "@tsva/streams/generator-pulling-stream-provider";
import { MemoryStreamProvider } from "@tsva/streams/memory-stream-provider";
import type { RedisClient } from "@tsva/streams/redis-pulling-stream-provider";
import { RedisPullingStreamProvider } from "@tsva/streams/redis-pulling-stream-provider";
import { StreamProviderConfigurationError } from "@tsva/streams/stream-provider-config-error";

// Config validation happens synchronously in the constructor, before the
// client is ever touched, so a fake client is enough here — no real Redis
// connection needed to exercise the config-load-failure path.
const fakeClient = {} as RedisClient;

describe("stream provider configuration errors", () => {
  it("rejects a non-positive queueCount for a Redis pulling provider", () => {
    expect(() => new RedisPullingStreamProvider(fakeClient, "default", { queueCount: 0 })).toThrow(
      StreamProviderConfigurationError,
    );
  });

  it("rejects a fractional queueCount for a Redis pulling provider", () => {
    expect(
      () => new RedisPullingStreamProvider(fakeClient, "default", { queueCount: 2.5 }),
    ).toThrow(StreamProviderConfigurationError);
  });

  it("rejects a negative pollIntervalMs for a Redis pulling provider", () => {
    expect(
      () => new RedisPullingStreamProvider(fakeClient, "default", { pollIntervalMs: -1 }),
    ).toThrow(StreamProviderConfigurationError);
  });

  it("accepts a valid Redis pulling provider config", () => {
    expect(
      () => new RedisPullingStreamProvider(fakeClient, "default", { queueCount: 8 }),
    ).not.toThrow();
  });

  it("rejects a non-positive queueCount for a generator pulling provider", () => {
    expect(
      () =>
        new GeneratorPullingStreamProvider(
          "gen",
          { streamNamespace: "n", eventsInStream: 1 },
          { queueCount: 0 },
        ),
    ).toThrow(StreamProviderConfigurationError);
  });

  it("rejects an empty provider name for the memory provider", () => {
    expect(() => new MemoryStreamProvider("")).toThrow(StreamProviderConfigurationError);
  });

  it("carries the provider name on the error", () => {
    try {
      new RedisPullingStreamProvider(fakeClient, "chat-streams", { queueCount: -1 });
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(StreamProviderConfigurationError);
      expect((err as StreamProviderConfigurationError).providerName).toBe("chat-streams");
    }
  });
});
