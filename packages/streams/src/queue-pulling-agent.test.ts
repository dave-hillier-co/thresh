import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { QueuePullingAgent, type DeliverEvent } from "@thresh/streams/queue-pulling-agent";
import { RedisStreamQueue } from "@thresh/streams/redis-stream-queue";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  // `reconnectStrategy: false` is load-bearing, not tidiness: node-redis retries a refused
  // connection forever by default, so `connect()` below never settles when no Redis is
  // listening and this module-load probe hangs the whole suite instead of skipping it.
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    return probe;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return undefined;
  }
}

async function deleteAll(c: Client, match: string): Promise<void> {
  for await (const keys of c.scanIterator({ MATCH: match })) {
    if (keys.length > 0) await c.del(keys);
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

async function waitForCursor(
  queue: RedisStreamQueue,
  value: number,
  timeoutMs = 2000,
): Promise<void> {
  const start = Date.now();
  while ((await queue.getCursor()) !== value) {
    if (Date.now() - start > timeoutMs) throw new Error("waitForCursor: timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

const client = await reachable(REDIS_URL);
const prefix = `thresh-test:queue:${randomUUID()}`;
let queueSeq = 0;

// A distinct queue key per test keeps runs isolated within the shared Redis.
function newQueue(): RedisStreamQueue {
  return new RedisStreamQueue(client!, `${prefix}:q${queueSeq++}`);
}

interface Delivered {
  streamKey: string;
  event: unknown;
  token: number;
}

const agents: QueuePullingAgent[] = [];
function startAgent(queue: RedisStreamQueue, deliver: DeliverEvent): QueuePullingAgent {
  const agent = new QueuePullingAgent(queue, deliver, { pollIntervalMs: 10 });
  agents.push(agent);
  agent.start();
  return agent;
}

afterAll(async () => {
  for (const a of agents) a.stop();
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisStreamQueue + QueuePullingAgent", () => {
  beforeEach(() => {
    for (const a of agents.splice(0)) a.stop();
  });

  it("delivers appended entries to the sink in order, with their stream key", async () => {
    const queue = newQueue();
    await queue.append("room/general", "a");
    await queue.append("room/general", "b");
    await queue.append("room/other", "c");

    const got: Delivered[] = [];
    startAgent(
      queue,
      async (streamKey, event, token) => void got.push({ streamKey, event, token }),
    );
    await waitFor(() => got.length === 3);

    expect(got.map((d) => d.event)).toEqual(["a", "b", "c"]);
    expect(got.map((d) => d.streamKey)).toEqual(["room/general", "room/general", "room/other"]);
    expect(got.map((d) => d.token)).toEqual([1, 2, 3]); // monotonic, 1-based
  });

  it("commits the queue cursor after delivery", async () => {
    const queue = newQueue();
    await queue.append("s", "x");
    await queue.append("s", "y");
    const got: unknown[] = [];
    startAgent(queue, async (_k, event) => void got.push(event));
    await waitFor(() => got.length === 2);
    await waitForCursor(queue, 2);
    expect(await queue.getCursor()).toBe(2);
  });

  it("a fresh agent resumes from the committed cursor, delivering only new entries", async () => {
    const queue = newQueue();
    await queue.append("s", "m1");
    await queue.append("s", "m2");
    const first: unknown[] = [];
    const a1 = startAgent(queue, async (_k, event) => void first.push(event));
    await waitFor(() => first.length === 2);
    a1.stop();

    await queue.append("s", "m3");
    await queue.append("s", "m4");

    const second: unknown[] = [];
    startAgent(newQueueAt(queue), async (_k, event) => void second.push(event));
    await waitFor(() => second.length === 2);
    expect(second).toEqual(["m3", "m4"]); // resumed from cursor=2
  });

  it("redelivers when the sink throws (cursor not advanced past a failure)", async () => {
    const queue = newQueue();
    await queue.append("s", "boom");
    let attempts = 0;
    startAgent(queue, async () => {
      attempts++;
      if (attempts === 1) throw new Error("transient");
    });
    // First attempt throws (cursor stays 0), a later poll redelivers and succeeds.
    await waitFor(() => attempts >= 2);
    await waitForCursor(queue, 1);
    expect(await queue.getCursor()).toBe(1);
  });
});

// Build a second queue handle pointing at the same physical queue key.
function newQueueAt(queue: RedisStreamQueue): RedisStreamQueue {
  return new RedisStreamQueue(client!, queue.key);
}

// Shutdown behaviour needs a queue whose reads the test controls, so it fakes
// the queue (a true boundary) rather than using Redis.
describe("QueuePullingAgent shutdown", () => {
  it("stop() resolves only after the in-flight pump settles", async () => {
    let releaseRead: (entries: never[]) => void = () => undefined;
    let reads = 0;
    const queue = {
      getCursor: async () => 0,
      readAfter: () => {
        reads++;
        return new Promise<never[]>((r) => {
          releaseRead = r;
        });
      },
      commit: async () => undefined,
    };
    const agent = new QueuePullingAgent(queue, async () => undefined, { pollIntervalMs: 5 });
    agent.start();
    await new Promise((r) => setTimeout(r, 20));
    expect(reads).toBe(1);

    let stopped = false;
    const stopping = agent.stop().then(() => {
      stopped = true;
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(stopped).toBe(false); // the pump's read is still in flight

    releaseRead([]);
    await stopping;
    const readsAtStop = reads;
    await new Promise((r) => setTimeout(r, 30));
    expect(reads).toBe(readsAtStop); // no pump runs after stop resolves
  });

  it("contains a pump failure and keeps polling", async () => {
    let reads = 0;
    const queue = {
      getCursor: async () => 0,
      readAfter: async () => {
        reads++;
        if (reads === 1) throw new Error("transient read failure");
        return [];
      },
      commit: async () => undefined,
    };
    const errors: unknown[] = [];
    const agent = new QueuePullingAgent(queue, async () => undefined, {
      pollIntervalMs: 5,
      onPumpError: (err) => {
        errors.push(err);
      },
    });
    agent.start();
    await new Promise((r) => setTimeout(r, 50));
    await agent.stop();
    expect(reads).toBeGreaterThanOrEqual(2); // the failure did not stop the poll loop
    expect(errors).toHaveLength(1);
  });
});
