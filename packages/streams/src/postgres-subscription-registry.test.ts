import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";
import { PostgresSubscriptionRegistry } from "@thresh/streams/postgres-subscription-registry";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

async function reachable(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => undefined);
    return undefined;
  }
}

const pool = await reachable(PG_URL);
const prefix = `thresh_test_ps_${randomUUID().replace(/-/g, "")}`;
const makeRegistry = (provider = "default") =>
  new PostgresSubscriptionRegistry(pool!, prefix, provider);

const ids = (subs: GrainId[]) => subs.map((g) => g.toString()).sort();

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${prefix}_subscriptions`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresSubscriptionRegistry", () => {
  beforeEach(async () => {
    if (pool === undefined) return;
    await pool.query(`DROP TABLE IF EXISTS ${prefix}_subscriptions`);
  });

  it("records subscribers for a stream and lists them", async () => {
    const reg = makeRegistry();
    await reg.start();
    await reg.subscribe("room/general", new GrainId("ChatUser", "alice"));
    await reg.subscribe("room/general", new GrainId("ChatUser", "bob"));
    expect(ids(await reg.subscribers("room/general"))).toEqual(["ChatUser/alice", "ChatUser/bob"]);
  });

  it("is idempotent: re-subscribing does not duplicate", async () => {
    const reg = makeRegistry();
    await reg.start();
    const alice = new GrainId("ChatUser", "alice");
    await reg.subscribe("room/general", alice);
    await reg.subscribe("room/general", alice);
    expect(await reg.subscribers("room/general")).toHaveLength(1);
  });

  it("removes a subscriber on unsubscribe", async () => {
    const reg = makeRegistry();
    await reg.start();
    const alice = new GrainId("ChatUser", "alice");
    const bob = new GrainId("ChatUser", "bob");
    await reg.subscribe("room/general", alice);
    await reg.subscribe("room/general", bob);
    await reg.unsubscribe("room/general", alice);
    expect(ids(await reg.subscribers("room/general"))).toEqual(["ChatUser/bob"]);
  });

  it("isolates subscribers by stream", async () => {
    const reg = makeRegistry();
    await reg.start();
    await reg.subscribe("room/one", new GrainId("ChatUser", "alice"));
    await reg.subscribe("room/two", new GrainId("ChatUser", "bob"));
    expect(ids(await reg.subscribers("room/one"))).toEqual(["ChatUser/alice"]);
    expect(await reg.subscribers("room/empty")).toEqual([]);
  });

  it("isolates subscribers by provider even when sharing a table prefix", async () => {
    const a = makeRegistry("provider-a");
    const b = makeRegistry("provider-b");
    await a.start();
    await b.start();
    await a.subscribe("s", new GrainId("ChatUser", "alice"));
    await b.subscribe("s", new GrainId("ChatUser", "bob"));
    expect(ids(await a.subscribers("s"))).toEqual(["ChatUser/alice"]);
    expect(ids(await b.subscribers("s"))).toEqual(["ChatUser/bob"]);
  });

  it("round-trips the full grain id (kind and key), not just its string", async () => {
    const reg = makeRegistry();
    await reg.start();
    const guid = Guid.parse("00000000-0000-0000-0000-0000000000ab");
    const subscriber = new GrainId("Device", guid);
    await reg.subscribe("telemetry/x", subscriber);
    const [back] = await reg.subscribers("telemetry/x");
    expect(back?.equals(subscriber)).toBe(true);
  });
});
