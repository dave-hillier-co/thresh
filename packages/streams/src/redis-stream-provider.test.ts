import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { SequenceToken, type StreamHandler } from "@tsva/core/stream";
import { RedisStreamProvider } from "@tsva/streams/redis-stream-provider";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  const probe = createClient({ url });
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

const client = await reachable(REDIS_URL);
const prefix = `tsva-test:stream:${randomUUID()}`;

// Each provider gets its own poll loop; track them so they all stop.
const providers: RedisStreamProvider[] = [];
function newProvider(): RedisStreamProvider {
  const p = new RedisStreamProvider(client!, "default", { keyPrefix: prefix, pollIntervalMs: 10 });
  providers.push(p);
  return p;
}

function collector<T>(into: T[]): StreamHandler<T> {
  return { onNext: async (event) => void into.push(event) };
}

afterAll(async () => {
  for (const p of providers) await p.stop();
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisStreamProvider", () => {
  beforeEach(async () => {
    for (const p of providers.splice(0)) await p.stop();
    await deleteAll(client!, `${prefix}*`);
  });

  it("delivers published events to a subscriber in order", async () => {
    const stream = newProvider().getStream<string>("room", "general");
    const got: string[] = [];
    await stream.subscribe(collector(got), { consumerId: "c1" });
    await stream.publish("a");
    await stream.publish("b");
    await stream.publish("c");
    await waitFor(() => got.length === 3);
    expect(got).toEqual(["a", "b", "c"]);
  });

  it("fans out every event to multiple consumers", async () => {
    const stream = newProvider().getStream<string>("room", "general");
    const a: string[] = [];
    const b: string[] = [];
    await stream.subscribe(collector(a), { consumerId: "a" });
    await stream.subscribe(collector(b), { consumerId: "b" });
    await stream.publish("x");
    await stream.publish("y");
    await waitFor(() => a.length === 2 && b.length === 2);
    expect(a).toEqual(["x", "y"]);
    expect(b).toEqual(["x", "y"]);
  });

  it("isolates streams by namespace and key", async () => {
    const provider = newProvider();
    const room1 = provider.getStream<string>("room", "one");
    const room2 = provider.getStream<string>("room", "two");
    const other = provider.getStream<string>("other", "one");
    const got1: string[] = [];
    await room1.subscribe(collector(got1), { consumerId: "c" });
    await room2.publish("two-only");
    await other.publish("other-only");
    await room1.publish("one-only");
    await waitFor(() => got1.length === 1);
    expect(got1).toEqual(["one-only"]);
  });

  it("rewinds to a start token", async () => {
    const stream = newProvider().getStream<string>("room", "general");
    await stream.publish("a");
    await stream.publish("b");
    const got: string[] = [];
    // Start token 0 is before the first event (seqs are 1-based), so all replay.
    await stream.subscribe(collector(got), {
      consumerId: "rewind",
      startToken: new SequenceToken(0),
    });
    await waitFor(() => got.length === 2);
    expect(got).toEqual(["a", "b"]);
  });

  it("resumes a consumer's own cursor on a fresh provider, replaying only what it missed", async () => {
    const first = newProvider().getStream<string>("room", "general");
    const before: string[] = [];
    await first.subscribe(collector(before), { consumerId: "c1" });
    await first.publish("m1");
    await first.publish("m2");
    await waitFor(() => before.length === 2);

    // The silo "restarts": stop its poll loops but keep the durable cursor.
    await providers[0]!.stop();

    // More events arrive while the consumer is gone (publish via a fresh provider).
    const between = newProvider().getStream<string>("room", "general");
    await between.publish("m3");
    await between.publish("m4");

    // A new silo reactivates the consumer: it reacquires its own subscription
    // from Redis and resumes from where it left off.
    const after = newProvider().getStream<string>("room", "general");
    const resumed: string[] = [];
    const handles = await after.getSubscriptions("c1");
    expect(handles).toHaveLength(1);
    await handles[0]!.resume(collector(resumed));
    await waitFor(() => resumed.length === 2);
    expect(resumed).toEqual(["m3", "m4"]); // only the missed events, no duplicates
  });

  it("returns no subscriptions for a consumer that never subscribed", async () => {
    const stream = newProvider().getStream<string>("room", "general");
    expect(await stream.getSubscriptions("nobody")).toHaveLength(0);
  });
});
