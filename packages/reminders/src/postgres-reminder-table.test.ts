import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { serializeValue } from "@thresh/core/value-codec";
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

  it("recordFired persists lastFiredAt only when the etag still matches", async () => {
    const table = makeTable();
    const etag = await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { hours: 1 },
    });

    expect(
      await table.recordFired(billing, "invoice", "wrong-etag", new Date(1000)),
    ).toBeUndefined();
    expect((await table.read(billing, "invoice"))?.lastFiredAt).toBeUndefined();

    expect(await table.recordFired(billing, "invoice", etag, new Date(1000))).toBe(etag);
    expect((await table.read(billing, "invoice"))?.lastFiredAt).toEqual(new Date(1000));
  });

  it("upsert clears a previously recorded lastFiredAt on re-registration", async () => {
    const table = makeTable();
    const etag1 = await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { hours: 1 },
    });
    await table.recordFired(billing, "invoice", etag1, new Date(1000));
    expect((await table.read(billing, "invoice"))?.lastFiredAt).toEqual(new Date(1000));

    await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(500),
      period: { hours: 2 },
    });
    expect((await table.read(billing, "invoice"))?.lastFiredAt).toBeUndefined();
  });
});

describe.skipIf(pool === undefined)("LocalReminderService over PostgresReminderTable", () => {
  const flush = () => new Promise((r) => setTimeout(r, 0));

  // Real DB round trips (recordFired, then reconcile's readRange) don't
  // always settle within a single microtask flush under load; poll with
  // real delay instead of assuming one tick suffices.
  async function waitFor(
    predicate: () => boolean | Promise<boolean>,
    timeoutMs = 5000,
  ): Promise<void> {
    const start = Date.now();
    while (!(await predicate())) {
      if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
      await new Promise((r) => setTimeout(r, 10));
    }
  }

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

  it("does not double-fire when a fresh silo reconciles the range after a tick (issue: reminder double-fire on rebalance)", async () => {
    const time = new FakeTimeProvider();
    const fired: number[] = [];
    const onFire = async (): Promise<void> => {
      fired.push(time.now());
    };

    const original = new LocalReminderService(makeTable(), time, onFire, [WHOLE]);
    await original.register(billing, "invoice", { ms: 1000 }, { ms: 1_000_000 });
    time.advance(1000); // first tick
    await waitFor(() => fired.length >= 1);
    expect(fired).toEqual([1000]);
    original.stop();

    // Wait for the (real, non-fake-clock-gated) recordFired write to land
    // before a fresh instance reconciles the same row.
    await waitFor(
      async () => (await makeTable().read(billing, "invoice"))?.lastFiredAt !== undefined,
    );

    // A successor takes over the same range from the durable table.
    const successor = new LocalReminderService(makeTable(), time, onFire, []);
    await successor.refreshOwnership([WHOLE]);
    await flush();
    expect(fired).toEqual([1000]); // no spurious immediate re-fire

    // The real next period boundary: startAt=1000 + period=1_000_000 =
    // t=1,001,000; the clock is currently at t=1000.
    time.advance(1_000_000);
    await waitFor(() => fired.length >= 2);
    expect(fired).toEqual([1000, 1_001_000]);
    successor.stop();
  });
});

// Issue #64: PostgresReminderTable partitions only by table name, so two
// services sharing one table with default names silently share reminder rows
// — the same bug #59 fixed for grain state. This mirrors #59's two-halved
// coverage: the partitioning a `serviceId` buys, and the in-place migration
// that keeps a table written under the old (pre-service_id) shape readable —
// with its backfill value agreeing with what a no-`serviceId` reader sees.
describe.skipIf(pool === undefined)(
  "PostgresReminderTable service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      await makeTable().start();
      await pool!.query(`TRUNCATE TABLE ${table}`);
    });

    it("keeps two service ids on separate rows for the same grain and reminder name", async () => {
      const alpha = new PostgresReminderTable(pool!, { tableName: table, serviceId: "alpha" });
      await alpha.start();
      await alpha.upsert({
        grainId: billing,
        name: "invoice",
        startAt: new Date(0),
        period: { hours: 1 },
      });

      const beta = new PostgresReminderTable(pool!, { tableName: table, serviceId: "beta" });
      await beta.start();
      expect(await beta.read(billing, "invoice")).toBeUndefined();
      expect(await beta.readForGrain(billing)).toEqual([]);
      const hash = billing.getUniformHashCode();
      expect(await beta.readRange(hash, hash + 1)).toEqual([]);

      const etag = await beta.upsert({
        grainId: billing,
        name: "invoice",
        startAt: new Date(0),
        period: { hours: 2 },
      });

      const alphaRead = await alpha.read(billing, "invoice");
      expect(alphaRead?.period).toEqual({ hours: 1 });
      const betaRead = await beta.read(billing, "invoice");
      expect(betaRead?.period).toEqual({ hours: 2 });
      expect(betaRead?.etag).toBe(etag);
    });

    it("migrates a table written under the pre-service_id schema, agreeing with the default reader", async () => {
      const legacy = `${table}_legacy`;
      await pool!.query(
        `CREATE TABLE ${legacy} (
         grain_id text NOT NULL,
         name text NOT NULL,
         hash bigint NOT NULL,
         data text NOT NULL,
         etag text NOT NULL,
         PRIMARY KEY (grain_id, name)
       )`,
      );
      try {
        const hash = billing.getUniformHashCode();
        await pool!.query(
          `INSERT INTO ${legacy} (grain_id, name, hash, data, etag) VALUES ($1, $2, $3, $4, $5)`,
          [
            billing.toString(),
            "invoice",
            String(hash),
            serializeValue({
              grainId: billing,
              name: "invoice",
              startAt: new Date(0),
              period: { hours: 1 },
            }),
            "legacy-etag",
          ],
        );

        const migrated = new PostgresReminderTable(pool!, { tableName: legacy });
        await migrated.start(); // must ALTER the existing table, not skip it

        const read = await migrated.read(billing, "invoice");
        expect(read?.etag).toBe("legacy-etag");
        expect(read?.period).toEqual({ hours: 1 });

        expect(await migrated.readRange(hash, hash + 1)).toHaveLength(1);

        expect(await migrated.remove(billing, "invoice", "legacy-etag")).toBe(true);
        const etag = await migrated.upsert({
          grainId: billing,
          name: "invoice",
          startAt: new Date(0),
          period: { hours: 3 },
        });
        const reread = await migrated.read(billing, "invoice");
        expect(reread?.etag).toBe(etag);
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${legacy}`);
      }
    });

    it("survives several instances racing the migration concurrently", async () => {
      // Several concurrent starts (not just two) widen the window so the
      // run reliably lands instances between the winner's DROP CONSTRAINT
      // and its ADD PRIMARY KEY, exercising isAlreadyAbsent() (42704) and
      // isAlreadyPresent() (42P16) — the two benign codes addServiceIdColumn()
      // can see when it loses either half of that race.
      const legacy = `${table}_race`;
      await pool!.query(
        `CREATE TABLE ${legacy} (
         grain_id text NOT NULL,
         name text NOT NULL,
         hash bigint NOT NULL,
         data text NOT NULL,
         etag text NOT NULL,
         PRIMARY KEY (grain_id, name)
       )`,
      );
      try {
        await pool!.query(
          `INSERT INTO ${legacy} (grain_id, name, hash, data, etag) VALUES ($1, $2, $3, $4, $5)`,
          [
            billing.toString(),
            "invoice",
            String(billing.getUniformHashCode()),
            serializeValue({
              grainId: billing,
              name: "invoice",
              startAt: new Date(0),
              period: { hours: 1 },
            }),
            "legacy-etag",
          ],
        );

        const instances = Array.from(
          { length: 5 },
          () => new PostgresReminderTable(pool!, { tableName: legacy }),
        );
        await Promise.all(instances.map((instance) => instance.start()));

        const read = await instances[0]!.read(billing, "invoice");
        expect(read?.etag).toBe("legacy-etag");
      } finally {
        await pool!.query(`DROP TABLE IF EXISTS ${legacy}`);
      }
      // Five concurrent Postgres round-trips can occasionally run past
      // vitest's default 5s timeout under host load; the longer timeout is
      // a test-harness margin, not a correctness bound.
    }, 15000);
  },
);
