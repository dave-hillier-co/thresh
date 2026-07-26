import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { DurableStreamFailureHandler } from "@thresh/streams/stream-failure-store";
import { PostgresStreamFailureStore } from "@thresh/streams/postgres-stream-failure-store";

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
const prefix = `thresh_test_pf_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${prefix}_failures`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresStreamFailureStore", () => {
  beforeEach(async () => {
    if (pool === undefined) return;
    await pool.query(`DROP TABLE IF EXISTS ${prefix}_failures`);
  });

  it("records a failure and lists it back", async () => {
    const store = new PostgresStreamFailureStore(pool!, prefix);
    await store.start();
    await store.recordFailure({
      streamKey: "room/bad",
      event: "poison",
      token: 1,
      error: "boom",
      attempts: 3,
      failedAt: 100,
    });
    expect(await store.listFailures()).toEqual([
      {
        streamKey: "room/bad",
        event: "poison",
        token: 1,
        error: "boom",
        attempts: 3,
        failedAt: 100,
      },
    ]);
  });

  it("narrows listFailures to one stream", async () => {
    const store = new PostgresStreamFailureStore(pool!, prefix);
    await store.start();
    await store.recordFailure({
      streamKey: "room/a",
      event: 1,
      token: 1,
      error: "e1",
      attempts: 3,
      failedAt: 1,
    });
    await store.recordFailure({
      streamKey: "room/b",
      event: 2,
      token: 2,
      error: "e2",
      attempts: 3,
      failedAt: 2,
    });
    expect((await store.listFailures("room/b")).map((f) => f.streamKey)).toEqual(["room/b"]);
  });

  it("persists across a fresh store instance (durable, not in-memory)", async () => {
    const first = new PostgresStreamFailureStore(pool!, prefix);
    await first.start();
    await first.recordFailure({
      streamKey: "room/bad",
      event: "poison",
      token: 7,
      error: "boom",
      attempts: 3,
      failedAt: 42,
    });

    const second = new PostgresStreamFailureStore(pool!, prefix);
    expect(await second.listFailures()).toHaveLength(1);
  });

  it("plugs into DurableStreamFailureHandler like any other durable store", async () => {
    const store = new PostgresStreamFailureStore(pool!, prefix);
    await store.start();
    const handler = new DurableStreamFailureHandler(store, () => 42);

    await handler.onDeliveryFailure("room/bad", "poison", 7, new Error("always fails"), 3);

    expect(await store.listFailures()).toEqual([
      {
        streamKey: "room/bad",
        event: "poison",
        token: 7,
        error: "always fails",
        attempts: 3,
        failedAt: 42,
      },
    ]);
  });
});
