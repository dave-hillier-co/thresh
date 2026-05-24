import { describe, expect, it } from "vitest";
import { SequenceToken, type StreamHandler } from "@tsva/core/stream";
import { MemoryStreamProvider } from "@tsva/streams/memory-stream-provider";

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
});
