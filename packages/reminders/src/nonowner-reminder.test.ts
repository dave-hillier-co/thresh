import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { TickStatus } from "@thresh/core/reminder";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { LocalReminderService, type HashRange } from "@thresh/reminders/local-reminder-service";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";

const billing = new GrainId("Billing", "acct-1");
const flush = async () => {
  for (let i = 0; i < 30; i++) await Promise.resolve();
};

const HOUR = 3_600_000;
const REFRESH = 60_000;

/** Two services over one table: `owner` owns the grain's hash, `other` owns the rest of the ring. */
function twoServices(refresh = REFRESH) {
  const time = new FakeTimeProvider();
  const table = new MemoryReminderTable();
  const fired: Array<{ at: number; status: TickStatus }> = [];
  const hash = billing.getUniformHashCode();
  const ownsHash: HashRange = [hash, hash + 1];
  const ownsRest: HashRange = [hash + 1, hash];
  const owner = new LocalReminderService(
    table,
    time,
    async (_g, _n, status) => {
      fired.push({ at: status.currentTickAt.getTime(), status });
    },
    [ownsHash],
    refresh,
  );
  const other = new LocalReminderService(
    table,
    time,
    async () => {
      throw new Error("non-owner fired");
    },
    [ownsRest],
    refresh,
  );
  return { time, table, fired, owner, other, ownsHash, ownsRest };
}

describe("a reminder registered on a silo that does not own its range", () => {
  it("first fires within one refresh interval of its due time, not after a period", async () => {
    const { time, fired, owner, other, ownsHash, ownsRest } = twoServices();
    await owner.refreshOwnership([ownsHash]);
    await other.refreshOwnership([ownsRest]);
    const t0 = time.now();

    await other.register(billing, "invoice", { ms: 5_000 }, { ms: HOUR });
    time.advance(REFRESH); // the owner's periodic refresh discovers the row
    await flush();
    time.advance(0); // the prompt first tick is due now
    await flush();

    expect(fired).toHaveLength(1);
    const first = fired[0]!;
    expect(first.at - t0).toBeGreaterThanOrEqual(5_000);
    expect(first.at - t0).toBeLessThanOrEqual(5_000 + REFRESH);
    expect(first.status.firstTickAt.getTime()).toBe(t0 + 5_000);

    // After that first tick it is on its grid: the next one is startAt + one period.
    time.advance(HOUR);
    await flush();
    expect(fired).toHaveLength(2);
    expect(fired[1]!.at - t0).toBe(5_000 + HOUR);
  });

  it("keeps a reminder that has already fired on its grid after an ownership change", async () => {
    const { time, table, fired, owner, ownsHash } = twoServices();
    await owner.refreshOwnership([ownsHash]);
    const t0 = time.now();
    await owner.register(billing, "invoice", { ms: 5_000 }, { ms: HOUR });
    time.advance(5_000);
    await flush();
    expect(fired).toHaveLength(1);
    expect((await table.read(billing, "invoice"))?.lastFiredAt).toBeDefined();

    // A successor takes the range a while later: it must not re-fire or catch up.
    owner.stop();
    time.advance(20 * 60_000);
    const successor = new LocalReminderService(
      table,
      time,
      async (_g, _n, status) => {
        fired.push({ at: status.currentTickAt.getTime(), status });
      },
      [ownsHash],
      REFRESH,
    );
    await successor.refreshOwnership([ownsHash]);
    await flush();
    expect(fired).toHaveLength(1);

    time.advance(HOUR);
    await flush();
    expect(fired).toHaveLength(2);
    expect(fired[1]!.at - t0).toBe(5_000 + HOUR);
  });
});

describe("a reminder unregistered on a silo that does not own its range", () => {
  it("delivers no further tick from the owner", async () => {
    // A long refresh interval, so the owner cannot learn of the removal by refreshing.
    const { time, fired, owner, other, ownsHash, ownsRest } = twoServices(HOUR);
    await owner.refreshOwnership([ownsHash]);
    await other.refreshOwnership([ownsRest]);
    await other.register(billing, "invoice", { ms: 0 }, { ms: REFRESH });
    await owner.refreshOwnership([ownsHash]); // the owner discovers it
    time.advance(1);
    await flush();
    const before = fired.length;
    expect(before).toBeGreaterThanOrEqual(1);

    await other.unregister(billing, "invoice");
    time.advance(REFRESH * 3);
    await flush();
    expect(fired).toHaveLength(before);
  });
});
