import { describe, expect, it } from "vitest";
import { QueuePullingAgent, type StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import type { QueueEntry, RedisStreamQueue } from "@thresh/streams/redis-stream-queue";

/**
 * In-memory fake of `RedisStreamQueue` — Redis is the only true boundary in this
 * package, so faking it here keeps the test sociable while letting us drive
 * timing deterministically.
 */
class FakeQueue {
  private entries: QueueEntry[] = [];
  private cursor = 0;
  appends = 0;

  append(streamKey: string, event: unknown): number {
    const token = ++this.appends;
    this.entries.push({ token, streamKey, event });
    return token;
  }

  // RedisStreamQueue surface used by the agent.
  async getCursor(): Promise<number> {
    return this.cursor;
  }

  async readAfter(cursor: number, count: number): Promise<QueueEntry[]> {
    return this.entries.filter((e) => e.token > cursor).slice(0, count);
  }

  async commit(cursor: number): Promise<void> {
    this.cursor = cursor;
  }

  async seek(cursor: number): Promise<void> {
    this.cursor = cursor;
  }
}

interface Delivered {
  streamKey: string;
  event: unknown;
  token: number;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}

describe("QueuePullingAgent failure handling", () => {
  it("skips a permanently failing event after the retry budget and continues delivering subsequent events", async () => {
    const queue = new FakeQueue();
    queue.append("room/bad", "poison");
    queue.append("room/good", "next-a");
    queue.append("room/good", "next-b");

    const delivered: Delivered[] = [];
    const failures: Array<{ streamKey: string; token: number; attempts: number }> = [];

    const failureHandler: StreamFailureHandler = {
      async onDeliveryFailure(streamKey, _event, token, _error, attempts) {
        failures.push({ streamKey, token, attempts });
      },
    };

    let attempts = 0;
    const agent = new QueuePullingAgent(
      queue as unknown as RedisStreamQueue,
      async (streamKey, event, token) => {
        if (streamKey === "room/bad") {
          attempts++;
          throw new Error("always fails");
        }
        delivered.push({ streamKey, event, token });
      },
      {
        pollIntervalMs: 1,
        maxAttempts: 3,
        // Keep backoff tight so the test stays fast.
        retryBackoffMs: () => 1,
        failureHandler,
      },
    );

    agent.start();
    try {
      await waitFor(() => delivered.length === 2);
    } finally {
      agent.stop();
    }

    // (a) cursor advanced past the bad event
    expect(await queue.getCursor()).toBe(3);
    // (a) handler tried exactly `maxAttempts` times before skipping
    expect(attempts).toBe(3);
    // (b) other events were delivered
    expect(delivered.map((d) => d.event)).toEqual(["next-a", "next-b"]);
    // failure was reported via the seam exactly once
    expect(failures).toEqual([{ streamKey: "room/bad", token: 1, attempts: 3 }]);
  });

  it("does not skip a transient failure that succeeds within the retry budget", async () => {
    const queue = new FakeQueue();
    queue.append("s", "x");

    let attempts = 0;
    const failures: unknown[] = [];
    const agent = new QueuePullingAgent(
      queue as unknown as RedisStreamQueue,
      async () => {
        attempts++;
        if (attempts < 2) throw new Error("transient");
      },
      {
        pollIntervalMs: 1,
        maxAttempts: 3,
        retryBackoffMs: () => 1,
        failureHandler: {
          async onDeliveryFailure(...args) {
            failures.push(args);
          },
        },
      },
    );

    agent.start();
    try {
      await waitFor(() => attempts >= 2);
      const start = Date.now();
      while ((await queue.getCursor()) !== 1) {
        if (Date.now() - start > 2000) throw new Error("cursor never advanced");
        await new Promise((r) => setTimeout(r, 5));
      }
    } finally {
      agent.stop();
    }

    expect(await queue.getCursor()).toBe(1);
    expect(failures).toEqual([]);
  });
});
