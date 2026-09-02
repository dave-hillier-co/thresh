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
