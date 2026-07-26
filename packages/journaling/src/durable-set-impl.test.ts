import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { DurableSetImpl } from "@thresh/journaling/durable-set-impl";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

const id = new GrainId("Tags", "t1");

function makeSet<T>(storage: MemoryJournalStorage) {
  const manager = new StateMachineManagerImpl("journal", id, storage);
  const set = new DurableSetImpl<T>("s", manager);
  manager.register(set);
  return { manager, set };
}

describe("DurableSetImpl", () => {
  it("tracks add/has/delete/clear in memory", async () => {
    const { set } = makeSet<string>(new MemoryJournalStorage());
    expect(await set.add("a")).toBe(true);
    expect(await set.add("b")).toBe(true);
    expect(await set.add("a")).toBe(false); // already present
    expect(set.size).toBe(2);
    expect(set.has("a")).toBe(true);
    expect([...set].sort()).toEqual(["a", "b"]);

    expect(await set.delete("a")).toBe(true);
    expect(await set.delete("a")).toBe(false); // already gone
    expect(set.has("a")).toBe(false);
    expect(set.size).toBe(1);

    await set.clear();
    expect(set.size).toBe(0);
  });

  it("does not journal a redundant add or a missing delete", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeSet<number>(storage);
    await a.set.add(1);
    await a.set.add(1); // redundant
    await a.set.delete(2); // missing

    const b = makeSet<number>(storage);
    await b.manager.replay();
    expect([...b.set.values()]).toEqual([1]);
  });

  it("rebuilds from the log on a fresh activation", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeSet<number>(storage);
    await a.set.add(1);
    await a.set.add(2);
    await a.set.add(3);
    await a.set.delete(2);

    const b = makeSet<number>(storage);
    await b.manager.replay();
    expect([...b.set.values()].sort()).toEqual([1, 3]);
  });
});
