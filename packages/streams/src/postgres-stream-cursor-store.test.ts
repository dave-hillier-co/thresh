import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { PostgresStreamCursorStore } from "@thresh/streams/postgres-stream-cursor-store";

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
const prefix = `thresh_test_pc_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${prefix}_cursors`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresStreamCursorStore", () => {
  beforeEach(async () => {
    if (pool === undefined) return;
    await pool.query(`DROP TABLE IF EXISTS ${prefix}_cursors`);
  });

  it("starts at cursor 0 and commits advance it", async () => {
    const store = new PostgresStreamCursorStore(pool!, prefix);
    await store.start();
    expect(await store.getCursor("p", 0)).toBe(0);
    await store.commit("p", 0, 5);
    expect(await store.getCursor("p", 0)).toBe(5);
  });

  // Ownership-handoff regression: a stale commit from a de-owned pulling
  // agent racing the new owner's fresher commit must not rewind the cursor
  // and cause a whole batch to be redelivered.
  it("does not regress the cursor on a stale, smaller commit (ownership handoff)", async () => {
    const store = new PostgresStreamCursorStore(pool!, prefix);
    await store.start();
    await store.commit("p", 0, 10);
    await store.commit("p", 0, 4);
    expect(await store.getCursor("p", 0)).toBe(10);
  });

  // seek() is the deliberate escape hatch for RecoverableStreamDeliveryError's
  // checkpoint rewind — unlike commit(), it must go backwards on request.
  it("seek unconditionally rewinds the cursor", async () => {
    const store = new PostgresStreamCursorStore(pool!, prefix);
    await store.start();
    await store.commit("p", 0, 10);
    await store.seek("p", 0, 3);
    expect(await store.getCursor("p", 0)).toBe(3);
  });
});

// Issue #64: PostgresStreamCursorStore partitions only by table prefix, so
// two services sharing one table silently share committed cursors — the same
// collision #59 fixed for grain state. The migration case matters most here:
// a reset cursor silently redelivers or skips a whole queue, so the
// migration's backfill value must agree exactly with what a no-`serviceId`
// reader gets.
describe.skipIf(pool === undefined)(
  "PostgresStreamCursorStore service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      if (pool === undefined) return;
      await pool.query(`DROP TABLE IF EXISTS ${prefix}_cursors`);
    });

    it("isolates cursors by service id sharing a table and provider/queue", async () => {
      const alpha = new PostgresStreamCursorStore(pool!, prefix, "alpha");
      const beta = new PostgresStreamCursorStore(pool!, prefix, "beta");
      await alpha.start();
      await beta.start();
      await alpha.commit("p", 0, 42);
      expect(await beta.getCursor("p", 0)).toBe(0);
      await beta.commit("p", 0, 7);
      expect(await alpha.getCursor("p", 0)).toBe(42);
      expect(await beta.getCursor("p", 0)).toBe(7);
    });

    it("migrates a legacy cursor value, not resetting it to 0", async () => {
      const legacy = `${prefix}_legacy`;
      await pool!.query(
        `CREATE TABLE ${legacy}_cursors (
           provider TEXT NOT NULL,
           queue_idx INT NOT NULL,
           cursor BIGINT NOT NULL,
           PRIMARY KEY (provider, queue_idx)
         )`,
      );
      try {
        await pool!.query(
          `INSERT INTO ${legacy}_cursors (provider, queue_idx, cursor) VALUES ('p', 0, 42)`,
        );

        const migrated = new PostgresStreamCursorStore(pool!, legacy);
        await migrated.start(); // must ALTER the existing table, not skip it
        // A reset to 0 here would silently redeliver (or, with eager trim,
        // silently skip) the whole queue's retained history.
        expect(await migrated.getCursor("p", 0)).toBe(42);

        // A service-scoped store over the same migrated table sees nothing.
        const scoped = new PostgresStreamCursorStore(pool!, legacy, "beta");
        expect(await scoped.getCursor("p", 0)).toBe(0);

        // The migrated cursor still advances in place.
        await migrated.commit("p", 0, 43);
        expect(await migrated.getCursor("p", 0)).toBe(43);
        expect(await scoped.getCursor("p", 0)).toBe(0);
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${legacy}_cursors`);
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
        `CREATE TABLE ${legacy}_cursors (
           provider TEXT NOT NULL,
           queue_idx INT NOT NULL,
           cursor BIGINT NOT NULL,
           PRIMARY KEY (provider, queue_idx)
         )`,
      );
      try {
        await pool!.query(
          `INSERT INTO ${legacy}_cursors (provider, queue_idx, cursor) VALUES ('p', 0, 42)`,
        );

        const instances = Array.from(
          { length: 5 },
          () => new PostgresStreamCursorStore(pool!, legacy),
        );
        await Promise.all(instances.map((instance) => instance.start()));

        expect(await instances[0]!.getCursor("p", 0)).toBe(42);
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${legacy}_cursors`);
      }
      // Five concurrent Postgres round-trips can occasionally run past
      // vitest's default 5s timeout under host load; the longer timeout is
      // a test-harness margin, not a correctness bound.
    }, 15000);
  },
);
