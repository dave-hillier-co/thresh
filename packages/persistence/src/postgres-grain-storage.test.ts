import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage } from "@thresh/core/grain-storage";
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
