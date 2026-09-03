import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";
import { serializeValue } from "@thresh/core/value-codec";
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

// Issue #64: PostgresSubscriptionRegistry partitions only by table name and
// provider, so two services sharing one table with the same provider name
// silently share subscriptions — the same collision #59 fixed for grain
// state. This mirrors #59's two-halved coverage: the partitioning a
// `serviceId` buys, and the in-place migration that keeps a table written
// under the pre-service_id shape readable, with its backfill value agreeing
// with what a no-`serviceId` reader sees.
describe.skipIf(pool === undefined)(
  "PostgresSubscriptionRegistry service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      if (pool === undefined) return;
      await pool.query(`DROP TABLE IF EXISTS ${prefix}_subscriptions`);
    });

    it("isolates subscribers by service id even sharing table and provider", async () => {
      const alpha = new PostgresSubscriptionRegistry(pool!, prefix, "default", "alpha");
      const beta = new PostgresSubscriptionRegistry(pool!, prefix, "default", "beta");
      await alpha.start();
      await beta.start();
      await alpha.subscribe("s", new GrainId("ChatUser", "alice"));
      await beta.subscribe("s", new GrainId("ChatUser", "bob"));
      expect(ids(await alpha.subscribers("s"))).toEqual(["ChatUser/alice"]);
      expect(ids(await beta.subscribers("s"))).toEqual(["ChatUser/bob"]);
    });

    it("migrates a table written under the pre-service_id schema and still reads it", async () => {
      const legacy = `${prefix}_legacy`;
      await pool!.query(
        `CREATE TABLE ${legacy}_subscriptions (
           provider TEXT NOT NULL,
           stream_key TEXT NOT NULL,
           subscriber TEXT NOT NULL,
           PRIMARY KEY (provider, stream_key, subscriber)
         )`,
      );
      try {
        const alice = new GrainId("ChatUser", "alice");
        await pool!.query(
          `INSERT INTO ${legacy}_subscriptions (provider, stream_key, subscriber) VALUES ($1, $2, $3)`,
          ["default", "room/general", serializeValue(alice)],
        );

        const migrated = new PostgresSubscriptionRegistry(pool!, legacy, "default");
        await migrated.start(); // must ALTER the existing table, not skip it
        expect(ids(await migrated.subscribers("room/general"))).toEqual(["ChatUser/alice"]);

        // Also confirm the row is still writable under the (defaulted) service id.
        await migrated.subscribe("room/general", new GrainId("ChatUser", "bob"));
        expect(ids(await migrated.subscribers("room/general"))).toEqual([
          "ChatUser/alice",
          "ChatUser/bob",
        ]);
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${legacy}_subscriptions`);
      }
    });

    it("survives several instances racing the migration concurrently", async () => {
      // Several concurrent starts (not just two) widen the window so the
      // run reliably lands instances between the winner's DROP CONSTRAINT
      // and its ADD PRIMARY KEY, exercising isAlreadyAbsent() (42704) and
      // isAlreadyPresent() (42P16) — the two benign codes addServiceIdColumn()
      // can see when it loses either half of that race.
      const legacy = `${prefix}_race`;
      await pool!.query(
        `CREATE TABLE ${legacy}_subscriptions (
           provider TEXT NOT NULL,
           stream_key TEXT NOT NULL,
           subscriber TEXT NOT NULL,
           PRIMARY KEY (provider, stream_key, subscriber)
         )`,
      );
      try {
        const alice = new GrainId("ChatUser", "alice");
        await pool!.query(
          `INSERT INTO ${legacy}_subscriptions (provider, stream_key, subscriber) VALUES ($1, $2, $3)`,
          ["default", "room/general", serializeValue(alice)],
        );

        const instances = Array.from(
          { length: 5 },
          () => new PostgresSubscriptionRegistry(pool!, legacy, "default"),
        );
        await Promise.all(instances.map((instance) => instance.start()));

        expect(ids(await instances[0]!.subscribers("room/general"))).toEqual(["ChatUser/alice"]);
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${legacy}_subscriptions`);
      }
      // Five concurrent Postgres round-trips can occasionally run past
      // vitest's default 5s timeout under host load; the longer timeout is
      // a test-harness margin, not a correctness bound.
    }, 15000);
  },
);
