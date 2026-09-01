import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";
import { serializeValue } from "@thresh/core/value-codec";
import { RedisSubscriptionRegistry } from "@thresh/streams/redis-subscription-registry";

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

const client = await reachable(REDIS_URL);
const prefix = `thresh-test:subs:${randomUUID()}`;
const makeRegistry = () => new RedisSubscriptionRegistry(client!, prefix, "default");

const ids = (subs: GrainId[]) => subs.map((g) => g.toString()).sort();

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisSubscriptionRegistry", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("records subscribers for a stream and lists them", async () => {
    const reg = makeRegistry();
    await reg.subscribe("room/general", new GrainId("ChatUser", "alice"));
    await reg.subscribe("room/general", new GrainId("ChatUser", "bob"));
    expect(ids(await reg.subscribers("room/general"))).toEqual(["ChatUser/alice", "ChatUser/bob"]);
  });

  it("is idempotent: re-subscribing does not duplicate", async () => {
    const reg = makeRegistry();
    const alice = new GrainId("ChatUser", "alice");
    await reg.subscribe("room/general", alice);
    await reg.subscribe("room/general", alice);
    expect(await reg.subscribers("room/general")).toHaveLength(1);
  });

  it("removes a subscriber on unsubscribe", async () => {
    const reg = makeRegistry();
    const alice = new GrainId("ChatUser", "alice");
    const bob = new GrainId("ChatUser", "bob");
    await reg.subscribe("room/general", alice);
    await reg.subscribe("room/general", bob);
    await reg.unsubscribe("room/general", alice);
    expect(ids(await reg.subscribers("room/general"))).toEqual(["ChatUser/bob"]);
  });

  it("isolates subscribers by stream", async () => {
    const reg = makeRegistry();
    await reg.subscribe("room/one", new GrainId("ChatUser", "alice"));
    await reg.subscribe("room/two", new GrainId("ChatUser", "bob"));
    expect(ids(await reg.subscribers("room/one"))).toEqual(["ChatUser/alice"]);
    expect(await reg.subscribers("room/empty")).toEqual([]);
  });

  it("round-trips the full grain id (kind and key), not just its string", async () => {
    const reg = makeRegistry();
    const guid = Guid.parse("00000000-0000-0000-0000-0000000000ab");
    const subscriber = new GrainId("Device", guid);
    await reg.subscribe("telemetry/x", subscriber);
    const [back] = await reg.subscribers("telemetry/x");
    expect(back?.equals(subscriber)).toBe(true);
  });
});

// Issue #64: RedisSubscriptionRegistry partitions only by keyPrefix and
// provider, so two services sharing one Redis with the same provider name
// silently share subscriptions. Redis has no ALTER, so this is a deliberate
// upgrade break (mirrors #59's RedisGrainStorage call): a set written under
// the old key shape orphans on upgrade.
describe.skipIf(client === undefined)(
  "RedisSubscriptionRegistry service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      await deleteAll(client!, `${prefix}*`);
    });

    it("isolates subscribers by service id sharing prefix and provider", async () => {
      const alpha = new RedisSubscriptionRegistry(client!, prefix, "default", "alpha");
      const beta = new RedisSubscriptionRegistry(client!, prefix, "default", "beta");
      await alpha.subscribe("s", new GrainId("ChatUser", "alice"));
      await beta.subscribe("s", new GrainId("ChatUser", "bob"));
      expect(ids(await alpha.subscribers("s"))).toEqual(["ChatUser/alice"]);
      expect(ids(await beta.subscribers("s"))).toEqual(["ChatUser/bob"]);
    });

    it("orphans a set written under the pre-service-dimension key shape (deliberate upgrade break)", async () => {
      // Old shape had no {serviceId} segment: `${prefix}:subs:{provider}:{streamKey}`.
      const alice = new GrainId("ChatUser", "alice");
      await client!.sAdd(`${prefix}:subs:default:room/general`, serializeValue(alice));

      const defaultConfigured = new RedisSubscriptionRegistry(client!, prefix, "default");
      expect(await defaultConfigured.subscribers("room/general")).toEqual([]);
    });
  },
);
