import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { LocalReminderService, type HashRange } from "@thresh/reminders/local-reminder-service";
import { RedisReminderTable } from "@thresh/reminders/redis-reminder-table";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  // `reconnectStrategy: false` is load-bearing, not tidiness: node-redis retries a refused
  // connection forever by default, so `connect()` below never settles when no Redis is
  // listening and this module-load probe hangs the whole suite instead of skipping it.
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    return probe;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return undefined;
  }
}

async function deleteAll(c: Client, match: string): Promise<void> {
  for await (const keys of c.scanIterator({ MATCH: match })) {
    if (keys.length > 0) await c.del(keys);
  }
}

const client = await reachable(REDIS_URL);
const prefix = `thresh-test:rem:${randomUUID()}`;
const makeTable = () => new RedisReminderTable(client!, { keyPrefix: prefix });

const WHOLE: HashRange = [0, 0x1_0000_0000];
const billing = new GrainId("Billing", "acct-1");

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisReminderTable", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
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

describe.skipIf(client === undefined)("LocalReminderService over RedisReminderTable", () => {
  const flush = () => new Promise((r) => setTimeout(r, 0));

  // Real network round trips (recordFired, then reconcile's readRange) don't
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
    await deleteAll(client!, `${prefix}*`);
  });

  it("fires a due reminder durably recorded in Redis", async () => {
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

  it("a fresh silo taking over the range resumes firing from Redis", async () => {
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

// Issue #64: RedisReminderTable partitions only by keyPrefix, so two services
// sharing one Redis with default prefixes silently share reminder keys — the
// same collision #59 fixed for grain state. Redis has no ALTER, so (like
// RedisGrainStorage) this is a deliberate upgrade break: pre-existing keys
// under the old shape orphan on upgrade (see the pin test below), pinned in
// todo.md / docs/deviations.md.
describe.skipIf(client === undefined)("RedisReminderTable service partitioning (issue #64)", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("keeps two service ids on separate keys for the same grain and reminder name", async () => {
    const alpha = new RedisReminderTable(client!, { keyPrefix: prefix, serviceId: "alpha" });
    await alpha.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { hours: 1 },
    });

    const beta = new RedisReminderTable(client!, { keyPrefix: prefix, serviceId: "beta" });
    expect(await beta.read(billing, "invoice")).toBeUndefined();
    const hash = billing.getUniformHashCode();
    expect(await beta.readRange(hash, hash + 1)).toEqual([]);

    await beta.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { hours: 2 },
    });

    expect((await alpha.read(billing, "invoice"))?.period).toEqual({ hours: 1 });
    expect((await beta.read(billing, "invoice"))?.period).toEqual({ hours: 2 });
  });

  it("orphans keys written under the pre-service-dimension key shape (deliberate upgrade break)", async () => {
    // The old shape had no {serviceId} segment: `${prefix}:rem:index|g:...|e:...`.
    const oldIndexKey = `${prefix}:rem:index`;
    const oldEntryKey = `${prefix}:rem:e:${billing.toString()}invoice`;
    await client!.zAdd(oldIndexKey, {
      score: billing.getUniformHashCode(),
      value: `${billing.toString()}invoice`,
    });
    await client!.hSet(oldEntryKey, {
      data: JSON.stringify({ grainId: billing.toString(), name: "invoice" }),
      etag: "old-etag",
    });

    const defaultConfigured = new RedisReminderTable(client!, { keyPrefix: prefix });
    expect(await defaultConfigured.read(billing, "invoice")).toBeUndefined();
  });
});
