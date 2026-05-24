import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { TickStatus } from "@tsva/core/reminder";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { LocalReminderService, type HashRange } from "@tsva/reminders/local-reminder-service";
import { MemoryReminderTable } from "@tsva/reminders/memory-reminder-table";

const WHOLE: HashRange = [0, 0x1_0000_0000];
const billing = new GrainId("Billing", "acct-1");

interface Fired {
  grainId: GrainId;
  name: string;
  status: TickStatus;
}

function recorder() {
  const fired: Fired[] = [];
  return {
    fired,
    onFire: async (grainId: GrainId, name: string, status: TickStatus) => {
      fired.push({ grainId, name, status });
    },
  };
}

describe("MemoryReminderTable", () => {
  it("upserts, reads and removes with etag CAS", async () => {
    const table = new MemoryReminderTable();
    const etag = await table.upsert({
      grainId: billing,
      name: "invoice",
      startAt: new Date(0),
      period: { hours: 24 },
    });
    expect((await table.read(billing, "invoice"))?.etag).toBe(etag);
    expect(await table.remove(billing, "invoice", "wrong-etag")).toBe(false);
    expect(await table.remove(billing, "invoice", etag)).toBe(true);
    expect(await table.read(billing, "invoice")).toBeUndefined();
  });

  it("reads only the entries whose grain hashes into a range", async () => {
    const table = new MemoryReminderTable();
    await table.upsert({ grainId: billing, name: "r", startAt: new Date(0), period: { ms: 0 } });
    const hash = billing.getUniformHashCode();
    expect(await table.readRange(hash, hash + 1)).toHaveLength(1);
    expect(await table.readRange(hash + 1, hash + 2)).toHaveLength(0);
  });
});

describe("LocalReminderService", () => {
  it("fires a due reminder, then again each period", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const { fired, onFire } = recorder();
    const service = new LocalReminderService(table, time, onFire, [WHOLE]);

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    time.advance(3000);
    expect(fired).toHaveLength(3);
    expect(fired[0]!.name).toBe("invoice");
    expect(fired[0]!.status.currentTickAt.getTime()).toBe(1000);
  });

  it("stops firing after unregister", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const { fired, onFire } = recorder();
    const service = new LocalReminderService(table, time, onFire, [WHOLE]);

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    time.advance(1000);
    expect(fired).toHaveLength(1);
    await service.unregister(billing, "invoice");
    time.advance(5000);
    expect(fired).toHaveLength(1);
    expect(await table.read(billing, "invoice")).toBeUndefined();
  });

  it("does not fire reminders outside its owned range", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();
    const { fired, onFire } = recorder();
    const hash = billing.getUniformHashCode();
    // Own a range that excludes the billing grain's hash.
    const service = new LocalReminderService(table, time, onFire, [[hash + 1, hash + 2]]);

    await service.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    time.advance(5000);
    expect(fired).toHaveLength(0);
    // ...but the reminder is still durably recorded for the real owner to pick up.
    expect(await table.read(billing, "invoice")).toBeDefined();
  });

  it("a new silo taking over the range resumes firing from the table", async () => {
    const time = new FakeTimeProvider();
    const table = new MemoryReminderTable();

    // Original owner registers the reminder, then "dies".
    const original = new LocalReminderService(table, time, async () => undefined, [WHOLE]);
    await original.register(billing, "invoice", { ms: 1000 }, { ms: 1000 });
    original.stop();

    // A fresh silo takes over the range and picks the reminder up from the table.
    const { fired, onFire } = recorder();
    const successor = new LocalReminderService(table, time, onFire, []);
    await successor.refreshOwnership([WHOLE]);

    time.advance(1000);
    expect(fired).toHaveLength(1);
    expect(fired[0]!.grainId.equals(billing)).toBe(true);
  });
});
