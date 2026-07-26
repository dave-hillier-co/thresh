import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { LocalReminderService, type HashRange } from "@thresh/reminders/local-reminder-service";
import { PostgresReminderTable } from "@thresh/reminders/postgres-reminder-table";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

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
const table = `thresh_test_${randomUUID().replace(/-/g, "")}`;
const makeTable = () => new PostgresReminderTable(pool!, { tableName: table });

const WHOLE: HashRange = [0, 0x1_0000_0000];
const billing = new GrainId("Billing", "acct-1");

afterAll(async () => {
  if (pool === undefined) return;
  await pool.query(`DROP TABLE IF EXISTS ${table}`);
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresReminderTable", () => {
  beforeEach(async () => {
    await makeTable().start();
    await pool!.query(`TRUNCATE TABLE ${table}`);
  });

  it("upserts, reads and removes with etag CAS", async () => {
    const table = makeTable();
    const etag = await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { hours: 24 },
    });
    const read = await table.read(billing, "invoice");
    expect(read?.etag).toBe(etag);
    expect(read?.startAt).toBeInstanceOf(Date);
    expect(read?.period).toEqual({ hours: 24 });
    expect(await table.remove(billing, "invoice", "wrong-etag")).toBe(false);
    expect(await table.remove(billing, "invoice", etag)).toBe(true);
    expect(await table.read(billing, "invoice")).toBeUndefined();
  });

  it("reads only the entries whose grain hashes into a range", async () => {
    const table = makeTable();
    await table.upsert({ grainId: billing, name: "r", startAt: new Date(0), period: { ms: 0 } });
    const hash = billing.getUniformHashCode();
    expect(await table.readRange(hash, hash + 1)).toHaveLength(1);
    expect(await table.readRange(hash + 1, hash + 2)).toHaveLength(0);
  });

  it("reads entries from a range that wraps the ring", async () => {
    const table = makeTable();
    await table.upsert({ grainId: billing, name: "r", startAt: new Date(0), period: { ms: 0 } });
    const hash = billing.getUniformHashCode();
    // Wrap (begin > end) that includes the hash: hash >= begin holds.
    expect(await table.readRange(hash, hash - 1)).toHaveLength(1);
    // Wrap that excludes the hash: neither hash >= begin nor hash < end.
    expect(await table.readRange(hash + 1, hash)).toHaveLength(0);
  });

  it("lists every reminder registered for one grain", async () => {
    const table = makeTable();
    await table.upsert({ grainId: billing, name: "a", startAt: new Date(0), period: { ms: 0 } });
    await table.upsert({ grainId: billing, name: "b", startAt: new Date(0), period: { ms: 0 } });
    const other = new GrainId("Billing", "acct-2");
    await table.upsert({ grainId: other, name: "a", startAt: new Date(0), period: { ms: 0 } });

    const forBilling = await table.readForGrain(billing);
    expect(forBilling.map((e) => e.name).sort()).toEqual(["a", "b"]);
  });

  it("overwrites in place with a fresh etag on re-upsert", async () => {
    const table = makeTable();
    const first = await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { ms: 0 },
    });
    const second = await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(1000),
      period: { ms: 5 },
    });
    expect(second).not.toBe(first);
    expect(await table.remove(billing, "invoice", first)).toBe(false);
    const read = await table.read(billing, "invoice");
    expect(read?.etag).toBe(second);
    expect(read?.period).toEqual({ ms: 5 });
  });
});

describe.skipIf(pool === undefined)("LocalReminderService over PostgresReminderTable", () => {
  const flush = () => new Promise((r) => setTimeout(r, 0));

  beforeEach(async () => {
    await makeTable().start();
    await pool!.query(`TRUNCATE TABLE ${table}`);
  });

  it("fires a due reminder durably recorded in Postgres", async () => {
    const time = new FakeTimeProvider();
    const table = makeTable();
    const fired: string[] = [];
    const service = new LocalReminderService(
      table,
      time,
      async (_g, name) => {
        fired.push(name);
      },
      [WHOLE],
    );

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1_000_000 });
    time.advance(1000);
    await flush();
    expect(fired).toEqual(["invoice"]);
    service.stop();
  });

  it("a fresh silo taking over the range resumes firing from Postgres", async () => {
    const time = new FakeTimeProvider();

    // Original owner registers, then "dies" without firing.
    const original = new LocalReminderService(makeTable(), time, async () => undefined, [WHOLE]);
    await original.register(billing, "invoice", { ms: 1000 }, { ms: 1_000_000 });
    original.stop();

    // A successor reads the durable table and picks the reminder up.
    const fired: string[] = [];
    const successor = new LocalReminderService(
      makeTable(),
      time,
      async (_g, name) => {
        fired.push(name);
      },
      [],
    );
    await successor.refreshOwnership([WHOLE]);
    time.advance(1000);
    await flush();
    expect(fired).toEqual(["invoice"]);
    successor.stop();
  });
});
