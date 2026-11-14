import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { DurableValueImpl } from "@thresh/journaling/durable-value-impl";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

const id = new GrainId("Counter", "c1");

function makeValue<T>(storage: MemoryJournalStorage) {
  const manager = new StateMachineManagerImpl("journal", id, storage);
  const value = new DurableValueImpl<T>("v", manager);
  manager.register(value);
  return { manager, value };
}

describe("DurableValueImpl", () => {
  it("starts undefined and tracks set/clear in memory", async () => {
    const { value } = makeValue<number>(new MemoryJournalStorage());
    expect(value.value).toBeUndefined();
    value.set(7);
    expect(value.value).toBe(7);
    value.clear();
    expect(value.value).toBeUndefined();
  });

  it("rebuilds from the log on a fresh activation", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeValue<number>(storage);
    await a.manager.replay();
    a.value.set(1);
    a.value.set(2);
    a.value.set(3);
    await a.manager.writeState();

    const b = makeValue<number>(storage);
    await b.manager.replay();
    expect(b.value.value).toBe(3);
  });

  it("replays a clear as the final state", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeValue<number>(storage);
    a.value.set(9);
    a.value.clear();
    await a.manager.writeState();

    const b = makeValue<number>(storage);
    await b.manager.replay();
    expect(b.value.value).toBeUndefined();
  });

  it("round-trips runtime value types (Date) through the codec", async () => {
    const storage = new MemoryJournalStorage();
    const when = new Date("2026-05-27T10:00:00.000Z");
    const a = makeValue<Date>(storage);
    a.value.set(when);
    await a.manager.writeState();

    const b = makeValue<Date>(storage);
    await b.manager.replay();
    expect(b.value.value).toBeInstanceOf(Date);
    expect(b.value.value?.getTime()).toBe(when.getTime());
  });
});
