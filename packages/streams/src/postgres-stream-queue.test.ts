import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { PostgresStreamQueue } from "@thresh/streams/postgres-stream-queue";

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
const prefix = `thresh_test_pq_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${prefix}_events`);
  await pool.query(`DROP TABLE IF EXISTS ${prefix}_cursors`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresStreamQueue", () => {
  beforeEach(async () => {
    if (pool === undefined) return;
    await pool.query(`DROP TABLE IF EXISTS ${prefix}_events`);
    await pool.query(`DROP TABLE IF EXISTS ${prefix}_cursors`);
  });

  it("starts with cursor 0 and no entries", async () => {
    const queue = new PostgresStreamQueue(pool!, prefix, "p", 0);
    await queue.start();
    expect(await queue.getCursor()).toBe(0);
    expect(await queue.readAfter(0)).toEqual([]);
  });

  it("appends events with a strictly monotonic token and reads them back in order", async () => {
    const queue = new PostgresStreamQueue(pool!, prefix, "p", 0);
    await queue.start();
    const t1 = await queue.append("room/a", { text: "hi" });
    const t2 = await queue.append("room/a", { text: "there" });
    expect(t2).toBeGreaterThan(t1);

    const entries = await queue.readAfter(0);
    expect(entries).toEqual([
      { token: t1, streamKey: "room/a", event: { text: "hi" } },
      { token: t2, streamKey: "room/a", event: { text: "there" } },
    ]);
  });

  it("readAfter only returns entries strictly after the given cursor", async () => {
    const queue = new PostgresStreamQueue(pool!, prefix, "p", 0);
    await queue.start();
    const t1 = await queue.append("s", "a");
    const t2 = await queue.append("s", "b");
    expect(await queue.readAfter(t1)).toEqual([{ token: t2, streamKey: "s", event: "b" }]);
  });

  it("commit persists the cursor for a fresh queue instance (handoff resume)", async () => {
    const first = new PostgresStreamQueue(pool!, prefix, "p", 0);
    await first.start();
    const token = await first.append("s", "a");
    await first.commit(token);

    const second = new PostgresStreamQueue(pool!, prefix, "p", 0);
    expect(await second.getCursor()).toBe(token);
  });

  it("isolates entries and cursors by queue index", async () => {
    const q0 = new PostgresStreamQueue(pool!, prefix, "p", 0);
    const q1 = new PostgresStreamQueue(pool!, prefix, "p", 1);
    await q0.start();
    await q1.start();
    await q0.append("s", "on-queue-0");
    await q1.append("s", "on-queue-1");

    expect((await q0.readAfter(0)).map((e) => e.event)).toEqual(["on-queue-0"]);
    expect((await q1.readAfter(0)).map((e) => e.event)).toEqual(["on-queue-1"]);
  });

  it("isolates entries and cursors by provider name", async () => {
    const a = new PostgresStreamQueue(pool!, prefix, "provider-a", 0);
    const b = new PostgresStreamQueue(pool!, prefix, "provider-b", 0);
    await a.start();
    await b.start();
    await a.append("s", "for-a");
    await b.append("s", "for-b");

    expect((await a.readAfter(0)).map((e) => e.event)).toEqual(["for-a"]);
    expect((await b.readAfter(0)).map((e) => e.event)).toEqual(["for-b"]);
  });

  it("eagerly trims delivered rows below the committed cursor on commit (no retainFor)", async () => {
    const queue = new PostgresStreamQueue(pool!, prefix, "p", 0);
    await queue.start();
    const t1 = await queue.append("s", "a");
    await queue.append("s", "b");
    await queue.commit(t1);

    const res = await pool!.query(`SELECT id FROM ${prefix}_events WHERE provider = 'p'`);
    expect(res.rows.map((r: { id: string }) => Number(r.id))).not.toContain(t1);
  });

  it("does not fail commit when trim errors (never blocks a successful delivery)", async () => {
    const queue = new PostgresStreamQueue(pool!, prefix, "p", 0);
    await queue.start();
    const token = await queue.append("s", "a");
    // Drop the events table out from under the trim to force it to error;
    // commit must still resolve because trim failures are swallowed.
    await pool!.query(`DROP TABLE ${prefix}_events`);
    await expect(queue.commit(token)).resolves.toBeUndefined();
  });

  it("with retainFor set, keeps recently delivered rows and only trims older ones", async () => {
    const queue = new PostgresStreamQueue(pool!, prefix, "p", 0, { hours: 1 });
    await queue.start();
    const token = await queue.append("s", "a");
    await queue.commit(token);

    // Within the retention window: the row survives the trim.
    const res = await pool!.query(`SELECT id FROM ${prefix}_events WHERE provider = 'p'`);
    expect(res.rows.map((r: { id: string }) => Number(r.id))).toContain(token);
  });
});
