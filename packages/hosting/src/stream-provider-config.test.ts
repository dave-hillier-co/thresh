import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import type { StreamFilter } from "@thresh/core/stream";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo, type SiloBuilder } from "@thresh/hosting/silo-builder";
import { StreamProviderConfigurationError } from "@thresh/streams/stream-provider-config-error";
import { MemoryStreamProvider } from "@thresh/streams/memory-stream-provider";
import { GeneratorPullingStreamProvider } from "@thresh/streams/generator-pulling-stream-provider";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function builder(): SiloBuilder {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork());
}

const allowAll: StreamFilter = { shouldDeliver: () => true };

describe("stream provider resolution", () => {
  it("resolves the registered names, and the default when no name is given", () => {
    const main = new MemoryStreamProvider("default");
    const audit = new MemoryStreamProvider("audit");
    const host = builder()
      .useMemoryStreams("default", undefined, main)
      .useMemoryStreams("audit", undefined, audit)
      .build();
    expect(host.getStreamProvider()).toBe(main);
    expect(host.getStreamProvider("audit")).toBe(audit);
  });

  it("refuses a name that was never registered, naming it and listing the registered ones", () => {
    const host = builder().useMemoryStreams("default").useMemoryStreams("audit").build();
    const attempt = () => host.getStreamProvider("auditt");
    expect(attempt).toThrow(StreamProviderConfigurationError);
    expect(attempt).toThrow(/auditt/);
    expect(attempt).toThrow(/default, audit/);
  });

  it("refuses the default when only other providers are registered", () => {
    const host = builder().useMemoryStreams("audit").build();
    expect(() => host.getStreamProvider()).toThrow(/"default"/);
  });
});

describe("stream filters", () => {
  it("honours every filter registered for a memory provider, not only the first", async () => {
    const provider = new MemoryStreamProvider("default");
    const delivered: unknown[] = [];
    const host = builder()
      .useMemoryStreams("default", undefined, provider)
      .addStreamFilter("default", { shouldDeliver: (_id, event) => event !== "blocked-by-first" })
      .addStreamFilter("default", { shouldDeliver: (_id, event) => event !== "blocked-by-second" })
      .build();
    provider.setDeliver(async (_subscriber, _key, event) => void delivered.push(event));
    provider.setImplicitSubscribers(() => ["Watcher"]);
    void host;

    const stream = provider.getStream<string>("chat", "general");
    await stream.publish("blocked-by-first");
    await stream.publish("blocked-by-second");
    await stream.publish("ok");
    expect(delivered).toEqual(["ok"]);
  });

  it("refuses at build a filter for a provider that is not registered", () => {
    const attempt = () =>
      builder().useMemoryStreams("default").addStreamFilter("nope", allowAll).build();
    expect(attempt).toThrow(StreamProviderConfigurationError);
    expect(attempt).toThrow(/nope/);
  });

  it("refuses at build a filter for a provider that cannot apply it", () => {
    const attempt = () =>
      builder()
        .addGeneratorStreams("gen", {
          streamNamespace: "gen",
          eventsInStream: 1,
        })
        .addStreamFilter("gen", allowAll)
        .build();
    expect(attempt).toThrow(StreamProviderConfigurationError);
    expect(attempt).toThrow(/gen/);
  });

  it("refuses at build a filter for a custom provider it cannot apply to", () => {
    const attempt = () =>
      builder()
        .addStreamProvider(
          "custom",
          new GeneratorPullingStreamProvider("custom", {
            streamNamespace: "c",
            eventsInStream: 1,
          }),
        )
        .addStreamFilter("custom", allowAll)
        .build();
    expect(attempt).toThrow(StreamProviderConfigurationError);
  });
});
