import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger } from "@thresh/core/logger";
import type { ReminderEntry, ReminderTable } from "@thresh/core/reminder";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { LocalReminderService, type HashRange } from "@thresh/reminders/local-reminder-service";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";

const WHOLE: HashRange = [0, 0x1_0000_0000];
const billing = new GrainId("Billing", "acct-1");
const flush = () => new Promise((r) => setTimeout(r, 0));

interface LogEntry {
  message: string;
  fields: Record<string, unknown> | undefined;
}

function recordingLogger(): { logger: Logger; errors: LogEntry[] } {
  const errors: LogEntry[] = [];
  return {
    errors,
    logger: {
      debug: () => {},
      info: () => {},
      warn: () => {},
      error: (message, fields) => {
        errors.push({ message, fields });
      },
    },
  };
}

describe("LocalReminderService — error logging (no silent swallow)", () => {
  it("logs reconcile errors when the table fails during refresh", async () => {
    const time = new FakeTimeProvider();
    const table: ReminderTable = {
      upsert: async () => "etag",
      remove: async () => true,
      read: async () => undefined,
      readForGrain: async () => [],
      readRange: async () => {
        throw new Error("boom-readRange");
      },
      recordFired: async (_g, _n, etag) => etag,
    };
    const { logger, errors } = recordingLogger();
    const refresh = 1000;
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], refresh, {
      logger,
      minimumPeriod: { ms: 0 },
    });
    await service.refreshOwnership([WHOLE]).catch(() => undefined);
    errors.length = 0; // ignore the initial reconcile error from refreshOwnership

    time.advance(refresh);
    await flush();

    expect(errors.length).toBeGreaterThan(0);
    expect(errors.some((e) => /reconcile/i.test(e.message))).toBe(true);
    service.stop();
  });

  it("logs onFire errors instead of swallowing them", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const { logger, errors } = recordingLogger();
    const service = new LocalReminderService(
      table,
      time,
      async () => {
        throw new Error("boom-onFire");
      },
      [WHOLE],
      0,
      { logger, minimumPeriod: { ms: 0 } },
    );

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    time.advance(1000);
    await flush();

    expect(errors.some((e) => /reminder/i.test(e.message))).toBe(true);
  });

  it("logs table.remove errors during one-shot cleanup", async () => {
    const time = new FakeTimeProvider();
    const memory = new MemoryReminderTable();
    const table: ReminderTable = {
      upsert: (r) => memory.upsert(r),
      remove: async () => {
        throw new Error("boom-remove");
      },
      read: (g, n) => memory.read(g, n),
      readForGrain: (g) => memory.readForGrain(g),
      readRange: (a, b) => memory.readRange(a, b),
      recordFired: (g, n, etag, at) => memory.recordFired(g, n, etag, at),
    };
    const { logger, errors } = recordingLogger();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], 0, {
      logger,
      minimumPeriod: { ms: 0 },
    });

    // One-shot reminder: period 0 triggers the table.remove cleanup path.
    await service.register(billing, "once", { ms: 1000 }, { ms: 0 });
    time.advance(1000);
    await flush();
    await flush();

    expect(errors.some((e) => /remove|cleanup/i.test(e.message))).toBe(true);
  });
});

describe("LocalReminderService — minimum period enforcement", () => {
  it("rejects a register call with period below the default minimum (1 minute)", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE]);

    await expect(
      service.register(billing, "invoice", { ms: 1000 }, { seconds: 30 }),
    ).rejects.toThrow(/minimum/i);
  });

  it("allows below-minimum periods when minimumPeriod override is configured", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });

    await expect(
      service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 }),
    ).resolves.toBeUndefined();
  });

  it("rejects negative periods", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });

    await expect(service.register(billing, "invoice", { ms: 1000 }, { ms: -1 })).rejects.toThrow();
  });
});

describe("LocalReminderService — GetReminder(s)", () => {
  it("getReminder returns the durable entry for the named reminder on the grain", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    const entry = await service.getReminder(billing, "invoice");
    expect(entry).toBeDefined();
    expect(entry!.name).toBe("invoice");
    expect(entry!.grainId.equals(billing)).toBe(true);
  });

  it("getReminder returns undefined for an unknown name", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    expect(await service.getReminder(billing, "nope")).toBeUndefined();
  });

  it("getReminders returns all reminders for the grain", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const service = new LocalReminderService(table, time, async () => undefined, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    const other = new GrainId("Billing", "acct-2");

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    await service.register(billing, "report", { ms: 1000 }, { ms: 1000 });
    await service.register(other, "invoice", { ms: 1000 }, { ms: 1000 });

    const entries = await service.getReminders(billing);
    const names = entries.map((e: ReminderEntry) => e.name).sort();
    expect(names).toEqual(["invoice", "report"]);
  });
});

describe("LocalReminderService — catch-up after downtime (initial due time)", () => {
  it("skips missed ticks and resumes on the startAt + n*period grid instead of firing immediately", async () => {
    // Orleans' CalculateInitialDueTime: a reminder whose first tick is long
    // past does not fire at once — it waits for the next startAt + n*period
    // boundary, and later ticks stay on that grid.
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const fires: number[] = [];
    const onFire = async (): Promise<void> => {
      fires.push(time.now());
    };

    const serviceA = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    // startAt = 1000, period = 1000.
    await serviceA.register(billing, "tick", { ms: 1000 }, { ms: 1000 });
    serviceA.stop(); // simulate the silo going down before the first tick

    // "Downtime" 0 -> 3400: no timer is live, so the clock just moves.
    time.advance(3400);

    // A fresh service takes over and reconciles from the table.
    const serviceB = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    await serviceB.refreshOwnership([WHOLE]);
    await flush();
    expect(fires).toEqual([]); // no catch-up fire for the missed ticks at 2000 and 3000

    time.advance(600); // grid boundary startAt + 3*period = 4000
    await flush();
    expect(fires).toEqual([4000]);

    time.advance(1000); // next tick stays on the grid: 5000, not 4000 + drift
    await flush();
    expect(fires).toEqual([4000, 5000]);

    serviceB.stop();
  });
});

describe("LocalReminderService — reconcile picks up updates from a non-owner", () => {
  it("replaces the locally scheduled reminder when the table etag differs", async () => {
    // A non-owner silo updates the reminder directly in the table (as
    // register() on that silo would, since it schedules locally only when it
    // owns the grain). The owner must pick up the new schedule on its next
    // reconcile instead of continuing to fire the stale one.
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const fires: number[] = [];
    const onFire = async (): Promise<void> => {
      fires.push(time.now());
    };

    const service = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    await service.register(billing, "tick", { ms: 1000 }, { ms: 1000 }); // owner: period 1000, first tick at t=1000

    // A non-owner writes a much shorter period straight to the table (it
    // doesn't own the grain, so it can't reschedule the owner's timer).
    await table.upsert({
      grainId: billing,
      name: "tick",
      // First tick at t=100 (a startAt of exactly now would be due now, as in
      // Orleans' CalculateInitialDueTime).
      startAt: new Date(time.now() + 100),
      period: { ms: 100 },
    });

    // The owner's periodic reconcile runs (or refreshOwnership on a view
    // change) before the stale timer would have fired at t=1000.
    await service.refreshOwnership([WHOLE]);
    await flush();
    expect(fires).toEqual([]); // no spurious fire from replacing the schedule

    time.advance(100); // the new period's first tick, not the old t=1000
    await flush();
    expect(fires).toEqual([100]);

    const entry = await service.getReminder(billing, "tick");
    expect(entry?.period).toEqual({ ms: 100 });

    service.stop();
  });
});

describe("LocalReminderService — double-fire on rebalance", () => {
  it("does not fire immediately when a fresh instance reconciles the same table after a tick", async () => {
    // Ownership-handoff hazard: without a persisted last-fired instant, a new
    // owner's reconcile() would recompute dueMs=0 from the original startAt
    // and fire immediately — a spurious fire for every reminder in the moved
    // range. It must instead resume at the correct next period boundary.
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const fires: number[] = [];
    const onFire = async (): Promise<void> => {
      fires.push(time.now());
    };

    const serviceA = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    await serviceA.register(billing, "tick", { ms: 1000 }, { ms: 1000 });

    time.advance(1000); // first tick at startAt (t=1000)
    await flush();
    expect(fires).toEqual([1000]);
    serviceA.stop();

    // Ownership moves to a fresh service instance over the same durable table.
    const serviceB = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    await serviceB.refreshOwnership([WHOLE]);
    await flush();
    expect(fires).toEqual([1000]); // no spurious immediate fire

    time.advance(1000); // the correct next period boundary (t=2000)
    await flush();
    expect(fires).toEqual([1000, 2000]);

    serviceB.stop();
  });
});

describe("LocalReminderService — catch-up after downtime for a reminder that has fired", () => {
  it("does not fire a catch-up tick when the last recorded tick is itself long past", async () => {
    // The issue's scenario: start 00:00, period 1h, ticks recorded, then the
    // cluster is down 00:50-02:20. Orleans' CalculateInitialDueTime works
    // from *now*, so the next tick is 03:00 — not an immediate 02:20 tick
    // computed from a stale lastFiredAt (00:00 -> 01:00, already past).
    const HOUR = 3_600_000;
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const fires: number[] = [];
    const onFire = async (): Promise<void> => {
      fires.push(time.now());
    };

    const serviceA = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    await serviceA.register(billing, "tick", { ms: 0 }, { ms: HOUR });
    time.advance(0);
    await flush();
    expect(fires).toEqual([0]); // the 00:00 tick, recorded as lastFiredAt
    expect((await table.read(billing, "tick"))?.lastFiredAt?.getTime()).toBe(0);

    time.advance(50 * 60_000);
    serviceA.stop(); // down at 00:50
    time.advance(90 * 60_000); // back at 02:20

    const serviceB = new LocalReminderService(table, time, onFire, [WHOLE], 0, {
      minimumPeriod: { ms: 0 },
    });
    await serviceB.refreshOwnership([WHOLE]);
    await flush();
    expect(fires).toEqual([0]); // no catch-up tick at 02:20

    time.advance(40 * 60_000); // 03:00
    await flush();
    expect(fires).toEqual([0, 3 * HOUR]);

    time.advance(HOUR); // 04:00
    await flush();
    expect(fires).toEqual([0, 3 * HOUR, 4 * HOUR]);
    serviceB.stop();
  });
});

describe("LocalReminderService — reconcile never applies a read older than a local registration", () => {
  /** Wraps a table so `readRange` returns its snapshot only once `release()` is called. */
  function gatedTable(inner: MemoryReminderTable): {
    table: ReminderTable;
    release: () => void;
    readStarted: Promise<void>;
  } {
    let release: () => void = () => undefined;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let started: () => void = () => undefined;
    const readStarted = new Promise<void>((r) => {
      started = r;
    });
    let gated = false;
    const table: ReminderTable = {
      upsert: (r) => inner.upsert(r),
      remove: (g, n, e) => inner.remove(g, n, e),
      read: (g, n) => inner.read(g, n),
      readForGrain: (g) => inner.readForGrain(g),
      recordFired: (g, n, e, f) => inner.recordFired(g, n, e, f),
      readRange: async (b, e) => {
        const snapshot = await inner.readRange(b, e);
        if (!gated) {
          gated = true;
          started();
          await gate;
        }
        return snapshot;
      },
    };
    return { table, release, readStarted };
  }

  it("keeps a re-registration made while a reconcile read was in flight", async () => {
    // Orleans guards ReadTableAndStartTimers with a local sequence number: a
    // table read that started before a local update must not replace it.
    const time = new FakeTimeProvider();
    const inner = new MemoryReminderTable();
    const { table, release, readStarted } = gatedTable(inner);
    const fires: number[] = [];
    const service = new LocalReminderService(
      table,
      time,
      async () => {
        fires.push(time.now());
      },
      [WHOLE],
      0,
      { minimumPeriod: { ms: 0 } },
    );
    await service.register(billing, "tick", { ms: 1000 }, { ms: 1000 });

    const refreshing = service.refreshOwnership([WHOLE]); // reads the old registration...
    await readStarted;
    await service.register(billing, "tick", { ms: 500 }, { ms: 500 }); // ...then it's updated locally
    release();
    await refreshing;

    time.advance(500);
    await flush();
    expect(fires).toEqual([500]); // the new schedule, not the stale read's t=1000
    service.stop();
  });

  it("keeps a new registration made while a reconcile read was in flight", async () => {
    const time = new FakeTimeProvider();
    const inner = new MemoryReminderTable();
    const { table, release, readStarted } = gatedTable(inner);
    const fires: number[] = [];
    const service = new LocalReminderService(
      table,
      time,
      async () => {
        fires.push(time.now());
      },
      [WHOLE],
      0,
      { minimumPeriod: { ms: 0 } },
    );

    const refreshing = service.refreshOwnership([WHOLE]); // snapshot: empty table
    await readStarted;
    await service.register(billing, "tick", { ms: 500 }, { ms: 500 });
    release();
    await refreshing;

    time.advance(500);
    await flush();
    expect(fires).toEqual([500]); // not cancelled by the stale (empty) read
    service.stop();
  });
});

describe("LocalReminderService — a fresh registration keeps its first tick", () => {
  it("fires a zero-due reminder at once even if the table write took a few ms", async () => {
    // The grid skip is for ticks missed while nobody owned the reminder; a
    // registration that just computed startAt = now + due missed nothing,
    // even when the upsert's latency leaves startAt slightly in the past.
    const time = new FakeTimeProvider();
    const inner = new MemoryReminderTable();
    const slowTable: ReminderTable = {
      upsert: async (r) => {
        time.advance(5);
        return inner.upsert(r);
      },
      remove: (g, n, e) => inner.remove(g, n, e),
      read: (g, n) => inner.read(g, n),
      readForGrain: (g) => inner.readForGrain(g),
      readRange: (b, e) => inner.readRange(b, e),
      recordFired: (g, n, e, f) => inner.recordFired(g, n, e, f),
    };
    const fires: number[] = [];
    const service = new LocalReminderService(
      slowTable,
      time,
      async () => {
        fires.push(time.now());
      },
      [WHOLE],
      0,
      { minimumPeriod: { ms: 0 } },
    );
    await service.register(billing, "tick", { ms: 0 }, { ms: 1000 });
    time.advance(0);
    await flush();
    expect(fires).toEqual([5]);

    time.advance(995); // back on the startAt + n*period grid: t=1000
    await flush();
    expect(fires).toEqual([5, 1000]);
    service.stop();
  });
});
