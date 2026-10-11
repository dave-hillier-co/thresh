import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { DurableValueImpl } from "@thresh/journaling/durable-value-impl";
import { PostgresJournalStorage } from "@thresh/journaling/postgres-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

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
// A unique table prefix isolates this run from anything else in the shared Postgres.
const tablePrefix = `thresh_test_journal_${randomUUID().replace(/-/g, "")}`;

const id = new GrainId("Agg", "g1");
const makeStorage = (serviceId?: string): PostgresJournalStorage =>
  new PostgresJournalStorage(pool!, {
    tablePrefix,
    ...(serviceId !== undefined ? { serviceId } : {}),
  });

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${tablePrefix}_entries`);
  await pool.query(`DROP TABLE IF EXISTS ${tablePrefix}_logs`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresJournalStorage", () => {
  beforeEach(async () => {
    await makeStorage().start();
    await pool!.query(`TRUNCATE TABLE ${tablePrefix}_entries, ${tablePrefix}_logs`);
  });

  it("start() is idempotent", async () => {
    await makeStorage().start();
    await makeStorage().start();
  });

  it("reads an empty log as no version", async () => {
    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual([]);
    expect(segment.version).toBeUndefined();
  });

  it("appends and round-trips through a fresh provider", async () => {
    const v1 = await makeStorage().append("journal", id, ["a", "b"], undefined);
    expect(v1).toBe(1);
    const v2 = await makeStorage().append("journal", id, ["c"], 1);
    expect(v2).toBe(2);

    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual(["a", "b", "c"]);
    expect(segment.version).toBe(2);
  });

  it("raises InconsistentStateError on a stale append", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    await expect(makeStorage().append("journal", id, ["b"], undefined)).rejects.toBeInstanceOf(
      InconsistentStateError,
    );
    await expect(makeStorage().append("journal", id, ["b"], 5)).rejects.toBeInstanceOf(
      InconsistentStateError,
    );
    // The losing appends left nothing behind.
    expect((await makeStorage().read("journal", id)).entries).toEqual(["a"]);
  });

  it("rejects an append against a log that does not exist yet", async () => {
    await expect(makeStorage().append("journal", id, ["a"], 1)).rejects.toBeInstanceOf(
      InconsistentStateError,
    );
  });

  it("lets exactly one of two racing appends win", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    const results = await Promise.allSettled([
      makeStorage().append("journal", id, ["b"], 1),
      makeStorage().append("journal", id, ["c"], 1),
    ]);
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    const rejected = results.filter((r) => r.status === "rejected");
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(InconsistentStateError);
    expect((await makeStorage().read("journal", id)).entries).toHaveLength(2);
  });

  it("replace truncates the log and bumps the version", async () => {
    await makeStorage().append("journal", id, ["a", "b", "c"], undefined);
    const v = await makeStorage().replace("journal", id, ["snap"], 1);
    expect(v).toBe(2);
    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual(["snap"]);
    expect(segment.version).toBe(2);

    // Appends after a replace continue the log in order.
    await makeStorage().append("journal", id, ["d"], 2);
    expect((await makeStorage().read("journal", id)).entries).toEqual(["snap", "d"]);
  });

  it("raises InconsistentStateError on a stale replace and keeps the log", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    await makeStorage().append("journal", id, ["b"], 1);
    await expect(makeStorage().replace("journal", id, ["snap"], 1)).rejects.toBeInstanceOf(
      InconsistentStateError,
    );
    expect((await makeStorage().read("journal", id)).entries).toEqual(["a", "b"]);
  });

  it("keeps logs of different names and grains apart", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    await makeStorage().append("other", id, ["x"], undefined);
    await makeStorage().append("journal", new GrainId("Agg", "g2"), ["y"], undefined);
    expect((await makeStorage().read("journal", id)).entries).toEqual(["a"]);
    expect((await makeStorage().read("other", id)).entries).toEqual(["x"]);
  });

  it("clear removes the log and its version", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    await makeStorage().clear("journal", id);
    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual([]);
    expect(segment.version).toBeUndefined();
    expect(await makeStorage().append("journal", id, ["b"], undefined)).toBe(1);
  });

  it("keeps two service ids' logs independent for the same grain and log name", async () => {
    await makeStorage("alpha").append("journal", id, ["a"], undefined);
    expect(await makeStorage("beta").read("journal", id)).toEqual({
      entries: [],
      version: undefined,
    });
    expect(await makeStorage("beta").append("journal", id, ["b"], undefined)).toBe(1);
    expect((await makeStorage("alpha").read("journal", id)).entries).toEqual(["a"]);
  });

  it("rejects a table prefix that is not a plain SQL identifier", () => {
    expect(() => new PostgresJournalStorage(pool!, { tablePrefix: "x; DROP TABLE y" })).toThrow();
  });

  it("rejects read() when the ambient signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(makeStorage().read("journal", id, controller.signal)).rejects.toBeDefined();
  });

  it("round-trips a full manager + structure through Postgres", async () => {
    const a = new StateMachineManagerImpl("journal", id, makeStorage());
    const av = new DurableValueImpl<number>("v", a);
    a.register(av);
    await a.replay();
    await av.set(1);
    await av.set(2);

    const b = new StateMachineManagerImpl("journal", id, makeStorage());
    const bv = new DurableValueImpl<number>("v", b);
    b.register(bv);
    await b.replay();
    expect(bv.value).toBe(2);
  });
});
