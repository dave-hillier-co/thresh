// L3: a stream delivery passes the same admission gate as a grain call. One
// that reaches a deactivating activation is held, never run there and never
// reported to the subscriber as an error, and reaches the next activation
// once it has resumed the subscription (Orleans reroutes the delivery message).
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { StreamHandler } from "@thresh/core/stream";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { LocalDispatcher } from "@thresh/runtime/local-dispatcher";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { MemoryStreamProvider } from "@thresh/streams/memory-stream-provider";

interface IConsumer extends GrainKey<string> {
  hold(): Promise<void>;
  tag(): Promise<string>;
}
const IConsumer = defineGrainInterface<IConsumer>("IConsumer.stream-reroute");

const received: string[] = [];
const errors: unknown[] = [];
let holdGate: Promise<void> = Promise.resolve();

@grain()
class ConsumerGrain extends Grain implements IConsumer {
  private readonly instanceTag = Math.random().toString(36).slice(2);

  override async onActivate(): Promise<void> {
    const handler: StreamHandler<number> = {
      onNext: async (event) => void received.push(`${this.instanceTag}:${event}`),
      onError: async (error) => void errors.push(error),
    };
    const stream = this.runtime.getStreamProvider().getStream<number>("ns", "k");
    const [existing] = await stream.getSubscriptions();
    if (existing !== undefined) await existing.resume(handler);
    else await stream.subscribe(handler);
  }
  async hold(): Promise<void> {
    await holdGate;
  }
  async tag(): Promise<string> {
    return this.instanceTag;
  }
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("a stream delivery reaching a deactivating activation (L3)", () => {
  it("is delivered to the next activation, not failed", async () => {
    const time = new FakeTimeProvider();
    const metadata = getGrainMetadata(ConsumerGrain)!;
    const provider = new MemoryStreamProvider();
    const factory = new GrainFactory(() => metadata.grainType, time);
    const catalog = new Catalog({
      grainTypes: new Map<string, RegisteredGrain>([
        [metadata.grainType, { ctor: ConsumerGrain, metadata }],
      ]),
      factory,
      time,
      defaultCollectionAgeSeconds: 600,
      streamProvider: () => provider,
    });
    factory.setDispatcher(new LocalDispatcher(catalog));
    const id = new GrainId(metadata.grainType, "c");
    const consumer = factory.getGrain(IConsumer, "c");
    const first = await consumer.tag();
    await flush();

    let release!: () => void;
    holdGate = new Promise<void>((r) => (release = r));
    const busy = consumer.hold();
    await flush();
    catalog.get(id)!.requestDeactivation();
    await provider.getStream<number>("ns", "k").publish(1);
    await flush();
    expect(received).toEqual([]); // held: not run on the deactivating activation

    release();
    await busy;
    const next = await consumer.tag(); // the next activation resumes the subscription
    expect(next).not.toBe(first);
    for (let i = 0; i < 5 && received.length === 0; i++) await flush();
    expect(received).toEqual([`${next}:1`]);
    expect(errors).toEqual([]);
  });
});
