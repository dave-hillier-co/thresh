import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import {
  PullingStreamProviderCore,
  type AppendableQueue,
  type SubscriptionRegistry,
} from "@thresh/streams/pulling-stream-provider-core";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";

/** In-memory queue — the backing store is the true boundary here. */
class FakeQueue implements AppendableQueue {
  private entries: QueueEntry[] = [];
  cursor = 0;

  async append(streamKey: string, event: unknown): Promise<number> {
    const token = this.entries.length + 1;
    this.entries.push({ token, streamKey, event });
    return token;
  }
  async getCursor(): Promise<number> {
    return this.cursor;
  }
  async readAfter(cursor: number, count = 128): Promise<QueueEntry[]> {
    return this.entries.filter((e) => e.token > cursor).slice(0, count);
  }
  async commit(cursor: number): Promise<void> {
    this.cursor = Math.max(this.cursor, cursor);
  }
  async seek(cursor: number): Promise<void> {
    this.cursor = cursor;
  }
}

class FakeRegistry implements SubscriptionRegistry {
  constructor(private readonly subs: GrainId[]) {}
  async subscribe(): Promise<void> {}
  async unsubscribe(): Promise<void> {}
  async subscribers(): Promise<GrainId[]> {
    return this.subs;
  }
}

const A = new GrainId("Consumer", "a");
const B = new GrainId("Consumer", "b");
const C = new GrainId("Consumer", "c");

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}

async function elapsedMs(work: Promise<unknown>): Promise<number> {
  const start = Date.now();
  await work;
  return Date.now() - start;
}

describe("PullingStreamProviderCore fan-out", () => {
  it("issue #97 repro: a failing middle subscriber neither re-delivers to earlier ones nor starves later ones", async () => {
    const queue = new FakeQueue();
    const core = new PullingStreamProviderCore("s", [queue], new FakeRegistry([A, B, C]), {
      pollIntervalMs: 1,
      maxEventDeliveryTimeMs: 50,
      retryBackoffMs: () => 5,
    });
    const received: Record<string, number> = { a: 0, b: 0, c: 0 };
    core.setDeliver(async (subscriber) => {
      const key = subscriber.key as string;
      received[key] = (received[key] ?? 0) + 1;
      if (subscriber.equals(B)) throw new Error("boom");
    });
    await queue.append("room/x", "evt");
    core.startAgentsFor([0]);
    try {
      await waitFor(() => queue.cursor === 1);
    } finally {
      await core.stop();
    }
    expect(received.a).toBe(1);
    expect(received.c).toBe(1);
    expect(received.b).toBeGreaterThan(1);
  });

  it("stop() abandons a subscriber's retry loop promptly instead of waiting out maxEventDeliveryTime", async () => {
    const queue = new FakeQueue();
    // Real (Orleans) default: 1-minute MaxEventDeliveryTime.
    const core = new PullingStreamProviderCore("s", [queue], new FakeRegistry([A, B]), {
      pollIntervalMs: 1,
      retryBackoffMs: () => 5,
    });
    let delivered = 0;
    let bAttempts = 0;
    core.setDeliver(async (subscriber) => {
      if (subscriber.equals(B)) {
        bAttempts++;
        throw new Error("boom");
      }
      delivered++;
    });
    await queue.append("room/x", "evt");
    core.startAgentsFor([0]);
    await waitFor(() => delivered === 1 && bAttempts >= 2);

    expect(await elapsedMs(core.stop())).toBeLessThan(1000);
    // The event was not committed past (nor reported as skipped) — the next
    // owner redelivers it from the committed cursor.
    expect(queue.cursor).toBe(0);
    const attemptsAtStop = bAttempts;
    await new Promise((r) => setTimeout(r, 50));
    expect(bAttempts).toBe(attemptsAtStop);
  });

  it("stop() does not wait out the response timeout of a hung onNext", async () => {
    const queue = new FakeQueue();
    const core = new PullingStreamProviderCore("s", [queue], new FakeRegistry([A]), {
      pollIntervalMs: 1,
    });
    let calls = 0;
    core.setDeliver(async () => {
      calls++;
      return new Promise<void>(() => {}); // hangs forever
    });
    await queue.append("room/x", "evt");
    core.startAgentsFor([0]);
    await waitFor(() => calls === 1);

    expect(await elapsedMs(core.stop())).toBeLessThan(1000);
    expect(queue.cursor).toBe(0);
  });
});
