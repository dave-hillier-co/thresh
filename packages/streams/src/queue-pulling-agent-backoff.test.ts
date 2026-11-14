import { describe, expect, it } from "vitest";
import type { Logger, LogFields } from "@thresh/core/logger";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { QueuePullingAgent, type PullableQueue } from "@thresh/streams/queue-pulling-agent";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";

/** A queue whose store can be switched off; every call while down is one recorded attempt. */
class FlakyQueue implements PullableQueue {
  down = true;
  attemptTimes: number[] = [];
  private entries: QueueEntry[] = [];
  private cursor = 0;

  constructor(private readonly time: FakeTimeProvider) {}

  append(streamKey: string, event: unknown): void {
    this.entries.push({ token: this.entries.length + 1, streamKey, event });
  }

  async getCursor(): Promise<number> {
    if (this.down) {
      this.attemptTimes.push(this.time.now());
      throw new Error("store down");
    }
    return this.cursor;
  }

  async readAfter(cursor: number, count: number): Promise<QueueEntry[]> {
    this.attemptTimes.push(this.time.now());
    return this.entries.filter((e) => e.token > cursor).slice(0, count);
  }

  async commit(cursor: number): Promise<void> {
    this.cursor = cursor;
  }

  async seek(cursor: number): Promise<void> {
    this.cursor = cursor;
  }
}

interface Logged {
  message: string;
  fields: LogFields | undefined;
}

function recordingLogger(errors: Logged[]): Logger {
  return {
    debug() {},
    info() {},
    warn() {},
    error: (message, fields) => void errors.push({ message, fields }),
  };
}

/** Let the in-flight pump's promise chain settle, without moving the clock. */
async function settle(): Promise<void> {
  for (let i = 0; i < 20; i += 1) await Promise.resolve();
}

async function advance(time: FakeTimeProvider, ms: number): Promise<void> {
  time.advance(ms);
  await settle();
}

describe("QueuePullingAgent poll failures", () => {
  it("backs off exponentially to the maximum delay, logging each failure once", async () => {
    const time = new FakeTimeProvider();
    const queue = new FlakyQueue(time);
    const errors: Logged[] = [];
    const agent = new QueuePullingAgent(queue, async () => undefined, {
      pollIntervalMs: 50,
      maxBackoffMs: 800,
      time,
      logger: recordingLogger(errors),
      random: () => 1,
    });
    agent.start();
    await advance(time, 0);

    // Advance 1ms at a time for a while: the gaps between attempts are the delays.
    for (let i = 0; i < 4_000; i += 1) await advance(time, 1);
    await agent.stop();

    const gaps = queue.attemptTimes.slice(1).map((t, i) => t - queue.attemptTimes[i]!);
    expect(gaps.slice(0, 5)).toEqual([50, 100, 200, 400, 800]);
    expect(Math.max(...gaps)).toBe(800);
    expect(gaps.slice(4).every((g) => g === 800)).toBe(true);
    expect(errors).toHaveLength(queue.attemptTimes.length);
    expect(errors[0]!.fields).toMatchObject({ error: expect.any(Error) });
  });

  it("jitters the delay so a fleet of agents does not retry in lockstep", async () => {
    const time = new FakeTimeProvider();
    const queue = new FlakyQueue(time);
    const agent = new QueuePullingAgent(queue, async () => undefined, {
      pollIntervalMs: 100,
      maxBackoffMs: 10_000,
      time,
      logger: recordingLogger([]),
      random: () => 0,
    });
    agent.start();
    await advance(time, 0);
    for (let i = 0; i < 400; i += 1) await advance(time, 1);
    await agent.stop();
    const gaps = queue.attemptTimes.slice(1).map((t, i) => t - queue.attemptTimes[i]!);
    // random() = 0 is the bottom of the jitter band: half the exponential delay.
    expect(gaps.slice(0, 2)).toEqual([50, 100]);
  });

  it("returns to the normal poll interval on the first success", async () => {
    const time = new FakeTimeProvider();
    const queue = new FlakyQueue(time);
    const errors: Logged[] = [];
    const delivered: unknown[] = [];
    const agent = new QueuePullingAgent(
      queue,
      async (_key, event) => void delivered.push(event),
      {
        pollIntervalMs: 50,
        maxBackoffMs: 800,
        time,
        logger: recordingLogger(errors),
        random: () => 1,
      },
    );
    agent.start();
    await advance(time, 0);
    for (let i = 0; i < 300; i += 1) await advance(time, 1);
    queue.down = false;
    queue.append("ns/k", "a");
    // Run until the pump after recovery has happened.
    for (let i = 0; i < 1_000 && delivered.length === 0; i += 1) await advance(time, 1);
    expect(delivered).toEqual(["a"]);

    const errorsAtRecovery = errors.length;
    const successAt = queue.attemptTimes[queue.attemptTimes.length - 1]!;
    for (let i = 0; i < 200; i += 1) await advance(time, 1);
    await agent.stop();
    const after = queue.attemptTimes.filter((t) => t >= successAt);
    const gaps = after.slice(1).map((t, i) => t - after[i]!);
    expect(gaps.length).toBeGreaterThanOrEqual(3);
    expect(gaps.every((g) => g === 50)).toBe(true);
    expect(errors).toHaveLength(errorsAtRecovery);
  });

  it("takes its waits from the injected time provider, not a raw timer", async () => {
    const time = new FakeTimeProvider();
    const queue = new FlakyQueue(time);
    queue.down = false;
    const agent = new QueuePullingAgent(queue, async () => undefined, {
      pollIntervalMs: 50,
      time,
    });
    agent.start();
    await advance(time, 0);
    const first = queue.attemptTimes.length;
    // No real time passes in this test; only the fake clock can fire the next poll.
    await new Promise((resolve) => setTimeout(resolve, 120));
    expect(queue.attemptTimes.length).toBe(first);
    await advance(time, 50);
    expect(queue.attemptTimes.length).toBe(first + 1);
    await agent.stop();
  });
});
