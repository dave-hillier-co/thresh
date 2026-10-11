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
// collision #59 fixed for grain state.
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

    it("survives several instances starting concurrently over a fresh table", async () => {
      const fresh = `${prefix}_race`;
      try {
        const instances = Array.from(
          { length: 5 },
          () => new PostgresStreamCursorStore(pool!, fresh),
        );
        await Promise.all(instances.map((instance) => instance.start()));

        await instances[0]!.commit("p", 0, 42);
        expect(await instances[1]!.getCursor("p", 0)).toBe(42);
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${fresh}_cursors`);
      }
      // Five concurrent Postgres round-trips can occasionally run past
      // vitest's default 5s timeout under host load; the longer timeout is
      // a test-harness margin, not a correctness bound.
    }, 15000);
  },
);
