import { describe, expect, it } from "vitest";
import { QueuePullingAgent } from "@thresh/streams/queue-pulling-agent";
import type { QueueEntry, RedisStreamQueue } from "@thresh/streams/redis-stream-queue";
import { RecoverableStreamDeliveryError } from "@thresh/streams/stream-recovery";

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
  // Per-subscriber retry-then-skip (issues #97/#98/#111) is `deliver`'s own
  // job now (`PullingStreamProviderCore.fanOut` → `FanOutDelivery`; see
  // `fan-out-delivery.test.ts`) — `deliver` never rejects for an ordinary
  // subscriber failure. What the agent itself still owns is: (a) a
  // queue-wide failure (deliver rejecting outright) is retried, unbounded,
  // by the ordinary poll loop, same as a queue read/commit failure; (b) a
  // `RecoverableStreamDeliveryError` seeks the queue's cursor back.

  it("retries a queue-wide delivery failure via the poll loop, unbounded, and continues once it stops failing", async () => {
    const queue = new FakeQueue();
    queue.append("room/good", "next-a");

    const delivered: Delivered[] = [];
    let calls = 0;
    const agent = new QueuePullingAgent(
      queue as unknown as RedisStreamQueue,
      async (streamKey, event, token) => {
        calls++;
        // The whole entry fails outright the first two polls (e.g. the
        // subscriber registry read itself failed) — not a per-subscriber
        // failure `fanOut` would have already absorbed.
        if (calls <= 2) throw new Error("registry unavailable");
        delivered.push({ streamKey, event, token });
      },
      { pollIntervalMs: 1 },
    );

    agent.start();
    try {
      await waitFor(() => delivered.length === 1);
    } finally {
      agent.stop();
    }

    // The cursor never advanced past the failing entry until it succeeded —
    // no skip, no bound on how many polls it took.
    expect(await queue.getCursor()).toBe(1);
    expect(delivered).toEqual([{ streamKey: "room/good", event: "next-a", token: 1 }]);
    expect(calls).toBeGreaterThanOrEqual(3);
  });

  it("seeks the queue back to the checkpoint on a RecoverableStreamDeliveryError, redelivering from there instead of skipping ahead", async () => {
    const queue = new FakeQueue();
    queue.append("s", "x");
    queue.append("s", "y");

    const events: unknown[] = [];
    let firstAttempt = true;
    const agent = new QueuePullingAgent(
      queue as unknown as RedisStreamQueue,
      async (_streamKey, event) => {
        if (event === "x" && firstAttempt) {
          firstAttempt = false;
          throw new RecoverableStreamDeliveryError("resync", 0);
        }
        events.push(event);
      },
      { pollIntervalMs: 1 },
    );

    agent.start();
    try {
      await waitFor(() => events.length === 2);
    } finally {
      agent.stop();
    }

    // "x" was rewound-and-redelivered (not permanently skipped), then "y".
    expect(events).toEqual(["x", "y"]);
    expect(await queue.getCursor()).toBe(2);
  });
});
