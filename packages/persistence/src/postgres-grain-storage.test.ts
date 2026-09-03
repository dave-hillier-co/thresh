import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage } from "@thresh/core/grain-storage";
import { serializeValue } from "@thresh/core/value-codec";
import { PersistentStateImpl } from "@thresh/persistence/persistent-state-impl";
import { PostgresGrainStorage } from "@thresh/persistence/postgres-grain-storage";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

/** Probe Postgres once at load time so the suite skips cleanly without it. */
async function reachable(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => {});
    return undefined;
  }
}

const pool = await reachable(PG_URL);
// A unique table isolates this run from anything else in the shared Postgres.
const table = `thresh_test_${randomUUID().replace(/-/g, "")}`;

interface Balance {
  cents: number;
}

const id = new GrainId("Account", "a1");
const makeStorage = (): GrainStorage => new PostgresGrainStorage(pool!, { tableName: table });
const makeState = (storage: GrainStorage, name = "balance") =>
  new PersistentStateImpl<Balance>(name, id, storage, () => ({ cents: 0 }));

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${table}`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresGrainStorage", () => {
  beforeEach(async () => {
    const storage = makeStorage() as PostgresGrainStorage;
    await storage.start();
    await pool!.query(`TRUNCATE TABLE ${table}`);
  });

  it("starts empty with a default value", async () => {
    const state = makeState(makeStorage());
    await state.read();
    expect(state.exists).toBe(false);
    expect(state.value.cents).toBe(0);
    expect(state.etag).toBeUndefined();
  });

  it("persists a write and reloads it on a fresh activation", async () => {
    const storage = makeStorage();
    const first = makeState(storage);
    first.value.cents = 100;
    await first.write();
    expect(first.exists).toBe(true);
    expect(first.etag).toBeDefined();

    // A fresh storage instance proves it round-trips through Postgres, not memory.
    const reactivated = makeState(makeStorage());
    await reactivated.read();
    expect(reactivated.value.cents).toBe(100);
  });

  it("raises InconsistentStateError when a stale writer loses the race", async () => {
    const storage = makeStorage();
    const a = makeState(storage);
    await a.read();
    a.value.cents = 1;
    await a.write();

    const b = makeState(makeStorage());
    await b.read(); // reads a's etag

    a.value.cents = 2;
    await a.write(); // bumps the etag

    b.value.cents = 3;
    await expect(b.write()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("rejects a blind write over an existing record", async () => {
    const a = makeState(makeStorage());
    a.value.cents = 1;
    await a.write();

    const blind = makeState(makeStorage()); // never read -> no etag
    blind.value.cents = 9;
    await expect(blind.write()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("clears the record and resets to the default", async () => {
    const storage = makeStorage();
    const state = makeState(storage);
    state.value.cents = 50;
    await state.write();
    await state.clear();
    expect(state.exists).toBe(false);
    expect(state.value.cents).toBe(0);

    const reloaded = makeState(makeStorage());
    await reloaded.read();
    expect(reloaded.exists).toBe(false);
  });

  it("treats clearing an absent record as a no-op", async () => {
    const state = makeState(makeStorage());
    await state.read();
    await expect(state.clear()).resolves.toBeUndefined();
    expect(state.exists).toBe(false);
  });

  it("rejects a stale clear", async () => {
    const a = makeState(makeStorage());
    a.value.cents = 1;
    await a.write();

    const b = makeState(makeStorage());
    await b.read(); // reads a's etag
    a.value.cents = 2;
    await a.write(); // bumps the etag

    await expect(b.clear()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("round-trips runtime value types (Date) through the codec", async () => {
    interface WithDate {
      at: Date;
    }
    const when = new Date("2026-05-25T09:00:00.000Z");
    const a = new PersistentStateImpl<WithDate>("dated", id, makeStorage(), () => ({
      at: new Date(0),
    }));
    a.value.at = when;
    await a.write();

    const b = new PersistentStateImpl<WithDate>("dated", id, makeStorage(), () => ({
      at: new Date(0),
    }));
    await b.read();
    expect(b.value.at).toBeInstanceOf(Date);
    expect(b.value.at.getTime()).toBe(when.getTime());
  });

  it("keeps named states independent", async () => {
    const balance = makeState(makeStorage(), "balance");
    balance.value.cents = 5;
    await balance.write();

    const limit = makeState(makeStorage(), "limit");
    await limit.read();
    expect(limit.exists).toBe(false);
  });
});

// Issue #59: Orleans' AdoNet provider keys a state row by ServiceId + GrainId
// (`PostgreSQL-Persistence.sql`, `OrleansStorage.serviceid`), so several
// clusters can share one database and stay partitioned. These two cases pin
// both halves of adding that dimension here: the partitioning it buys, and the
// in-place migration that keeps a table written under the old shape readable.
describe.skipIf(pool === undefined)("PostgresGrainStorage service partitioning (issue #59)", () => {
  beforeEach(async () => {
    const storage = makeStorage() as PostgresGrainStorage;
    await storage.start();
    await pool!.query(`TRUNCATE TABLE ${table}`);
  });

  it("keeps two service ids on separate rows for the same grain and state", async () => {
    const alpha = new PostgresGrainStorage(pool!, { tableName: table, serviceId: "alpha" });
    await alpha.start();
    const a = makeState(alpha);
    a.value.cents = 1;
    await a.write();

    // A second cluster has never seen this grain: its read must find nothing,
    // and its blind write must succeed, because alpha's row is not its row.
    const beta = new PostgresGrainStorage(pool!, { tableName: table, serviceId: "beta" });
    const b = makeState(beta);
    await b.read();
    expect(b.exists).toBe(false);
    b.value.cents = 2;
    await b.write();

    // Neither cluster observes the other's value.
    const reAlpha = makeState(
      new PostgresGrainStorage(pool!, { tableName: table, serviceId: "alpha" }),
    );
    await reAlpha.read();
    expect(reAlpha.value.cents).toBe(1);
    const reBeta = makeState(
      new PostgresGrainStorage(pool!, { tableName: table, serviceId: "beta" }),
    );
    await reBeta.read();
    expect(reBeta.value.cents).toBe(2);
  });

  it("migrates a table written under the pre-service_id schema and still reads its rows", async () => {
    const legacy = `${table}_legacy`;
    await pool!.query(
      `CREATE TABLE ${legacy} (
         grain_id text NOT NULL,
         state_name text NOT NULL,
         data text NOT NULL,
         etag text NOT NULL,
         PRIMARY KEY (grain_id, state_name)
       )`,
    );
    try {
      await pool!.query(
        `INSERT INTO ${legacy} (grain_id, state_name, data, etag) VALUES ($1, $2, $3, $4)`,
        [id.toString(), "balance", serializeValue({ cents: 77 }), "legacy-etag"],
      );

      const storage = new PostgresGrainStorage(pool!, { tableName: legacy });
      await storage.start(); // must ALTER the existing table, not skip it

      const state = makeState(storage);
      await state.read();
      expect(state.exists).toBe(true);
      expect(state.value.cents).toBe(77);
      expect(state.etag).toBe("legacy-etag");

      // The migrated row is still writable under its (defaulted) service id.
      state.value.cents = 78;
      await state.write();
      const reread = makeState(new PostgresGrainStorage(pool!, { tableName: legacy }));
      await reread.read();
      expect(reread.value.cents).toBe(78);
    } finally {
      await pool!.query(`DROP TABLE IF EXISTS ${legacy}`);
    }
  });

  it("survives several silos racing the migration concurrently", async () => {
    // Several concurrent starts (not just two) widen the window so the run
    // reliably lands both instances between the winner's DROP CONSTRAINT and
    // its ADD PRIMARY KEY, exercising isAlreadyAbsent() (42704) and
    // isAlreadyPresent() (42P16) — the two benign codes addServiceIdColumn()
    // can see when it loses either half of that race.
    const legacy = `${table}_race`;
    await pool!.query(
      `CREATE TABLE ${legacy} (
         grain_id text NOT NULL,
         state_name text NOT NULL,
         data text NOT NULL,
         etag text NOT NULL,
         PRIMARY KEY (grain_id, state_name)
       )`,
    );
    try {
      await pool!.query(
        `INSERT INTO ${legacy} (grain_id, state_name, data, etag) VALUES ($1, $2, $3, $4)`,
        [id.toString(), "balance", serializeValue({ cents: 77 }), "legacy-etag"],
      );

      const silos = Array.from(
        { length: 5 },
        () => new PostgresGrainStorage(pool!, { tableName: legacy }),
      );
      await Promise.all(silos.map((silo) => silo.start()));

      const state = makeState(new PostgresGrainStorage(pool!, { tableName: legacy }));
      await state.read();
      expect(state.exists).toBe(true);
      expect(state.value.cents).toBe(77);
      expect(state.etag).toBe("legacy-etag");
    } finally {
      await pool!.query(`DROP TABLE IF EXISTS ${legacy}`);
    }
    // Five concurrent Postgres round-trips can occasionally run past
    // vitest's default 5s timeout under host load; the longer timeout is a
    // test-harness margin, not a correctness bound.
  }, 15000);
});
