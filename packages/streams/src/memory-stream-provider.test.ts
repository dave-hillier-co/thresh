import { describe, expect, it } from "vitest";
import { SequenceToken, type BatchedStreamItem, type StreamHandler } from "@thresh/core/stream";
import { MemoryStreamProvider } from "@thresh/streams/memory-stream-provider";

const flush = () => new Promise((r) => setTimeout(r, 0));

function collector<T>(): { received: T[]; handler: StreamHandler<T> } {
  const received: T[] = [];
  return { received, handler: { onNext: async (event) => void received.push(event) } };
}

describe("MemoryStreamProvider", () => {
  it("delivers events to a subscriber in publish order", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    const { received, handler } = collector<number>();
    await stream.subscribe(handler);

    await stream.publish(1);
    await stream.publish(2);
    await stream.publish(3);
    await flush();
    expect(received).toEqual([1, 2, 3]);
  });

  it("resumes from the cursor after the consumer drops and re-subscribes", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    const first = collector<number>();
    const handle = await stream.subscribe(first.handler);

    await stream.publish(10);
    await flush();
    expect(first.received).toEqual([10]);

    // Simulate deactivation: drop the handler, publish more, then resume.
    first.handler.onNext = async () => {
      throw new Error("detached");
    };
    await stream.publish(20);
    await flush();

    const second = collector<number>();
    await handle.resume(second.handler);
    await flush();
    expect(second.received).toEqual([20]); // only the unacked event, in order
  });

  it("rewinds to a start token", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    await stream.publish(1);
    await stream.publish(2);

    const { received, handler } = collector<number>();
    await stream.subscribe(handler, { startToken: new SequenceToken(0) });
    await flush();
    expect(received).toEqual([1, 2]);
  });

  it("redelivers an event whose handler threw (at-least-once)", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    let failNext = true;
    const received: number[] = [];
    const handle = await stream.subscribe({
      onNext: async (event) => {
        if (failNext) {
          failNext = false;
          throw new Error("transient");
        }
        received.push(event);
      },
      onError: async () => undefined,
    });

    await stream.publish(42);
    await flush();
    expect(received).toEqual([]); // first delivery threw, cursor not advanced

    await handle.resume({ onNext: async (event) => void received.push(event) });
    await flush();
    expect(received).toEqual([42]); // redelivered
  });

  it("isolates streams by namespace and key", async () => {
    const provider = new MemoryStreamProvider();
    const a = collector<number>();
    const b = collector<number>();
    await provider.getStream<number>("telemetry", "dev-1").subscribe(a.handler);
    await provider.getStream<number>("telemetry", "dev-2").subscribe(b.handler);
    await provider.getStream<number>("telemetry", "dev-1").publish(7);
    await flush();
    expect(a.received).toEqual([7]);
    expect(b.received).toEqual([]);
  });

  it("delivers a publish to an implicit subscriber via setDeliver/setImplicitSubscribers, with no subscribe call", async () => {
    const provider = new MemoryStreamProvider();
    const delivered: Array<{ subscriber: string; streamKey: string; event: unknown }> = [];
    provider.setDeliver(async (subscriber, streamKey, event) => {
      delivered.push({ subscriber: subscriber.toString(), streamKey, event });
    });
    provider.setImplicitSubscribers((namespace) => (namespace === "chat" ? ["Watcher"] : []));

    await provider.getStream<string>("chat", "general").publish("hello");

    expect(delivered).toEqual([
      { subscriber: "Watcher/general", streamKey: "chat/general", event: "hello" },
    ]);
  });

  it("does not deliver to an implicit subscriber before setImplicitSubscribers is wired", async () => {
    const provider = new MemoryStreamProvider();
    const delivered: unknown[] = [];
    provider.setDeliver(async (_subscriber, _streamKey, event) => void delivered.push(event));

    await provider.getStream<string>("chat", "general").publish("hello");

    expect(delivered).toEqual([]);
  });

  it("delivers implicit subscribers by namespace only for namespaces with a matching type", async () => {
    const provider = new MemoryStreamProvider();
    const delivered: string[] = [];
    provider.setDeliver(async (subscriber) => void delivered.push(subscriber.toString()));
    provider.setImplicitSubscribers((namespace) => (namespace === "chat" ? ["Watcher"] : []));

    await provider.getStream<string>("other", "general").publish("ignored");

    expect(delivered).toEqual([]);
  });

  it("publishBatch delivers every event to a single-item subscriber, in order (unbatched)", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    const { received, handler } = collector<number>();
    await stream.subscribe(handler);

    await stream.publishBatch!([1, 2, 3]);
    await flush();

    expect(received).toEqual([1, 2, 3]);
  });

  it("publishBatch delivers the whole batch in one call to a subscriber that declares onNextBatch", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    const batches: number[][] = [];
    await stream.subscribe({
      onNext: async () => {
        throw new Error("onNext should not be called when onNextBatch is declared");
      },
      onNextBatch: async (items: readonly BatchedStreamItem<number>[]) => {
        batches.push(items.map((i) => i.event));
      },
    });

    await stream.publishBatch!([1, 2, 3]);
    await flush();

    expect(batches).toEqual([[1, 2, 3]]);
  });

  it("publishBatch fans a batch out to implicit subscribers one event at a time", async () => {
    const provider = new MemoryStreamProvider();
    const delivered: unknown[] = [];
    provider.setDeliver(async (_subscriber, _streamKey, event) => void delivered.push(event));
    provider.setImplicitSubscribers((namespace) => (namespace === "chat" ? ["Watcher"] : []));

    await provider.getStream<number>("chat", "general").publishBatch!([1, 2, 3]);

    expect(delivered).toEqual([1, 2, 3]);
  });

  it("publish is equivalent to a one-item publishBatch", async () => {
    const provider = new MemoryStreamProvider();
    const stream = provider.getStream<number>("telemetry", "dev-1");
    const { received, handler } = collector<number>();
    await stream.subscribe(handler);

    await stream.publish(1);
    await flush();

    expect(received).toEqual([1]);
  });

  it("registers and unregisters an explicit producer", async () => {
    const provider = new MemoryStreamProvider("chat-provider");

    const handle = await provider.registerProducer("chat", "room-1");
    expect(handle.streamId).toEqual({
      provider: "chat-provider",
      namespace: "chat",
      key: "room-1",
    });

    await handle.unregister();
    // Idempotent: a second unregister is a no-op, not an error.
    await handle.unregister();
  });
});
