import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { DurableListImpl } from "@thresh/journaling/durable-list-impl";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

const id = new GrainId("Feed", "f1");

function makeList<T>(storage: MemoryJournalStorage) {
  const manager = new StateMachineManagerImpl("journal", id, storage);
  const list = new DurableListImpl<T>("l", manager);
  manager.register(list);
  return { manager, list };
}

describe("DurableListImpl", () => {
  it("tracks add/set/removeAt/clear in memory", async () => {
    const { list } = makeList<string>(new MemoryJournalStorage());
    list.add("a");
    list.add("b");
    list.add("c");
    list.set(1, "B");
    list.removeAt(0);
    expect(list.toArray()).toEqual(["B", "c"]);
    expect([...list]).toEqual(["B", "c"]);
    list.clear();
    expect(list.length).toBe(0);
  });

  it("contains reports whether a value is present", async () => {
    const { list } = makeList<string>(new MemoryJournalStorage());
    list.add("one");
    list.add("two");
    list.add("three");

    expect(list.contains("two")).toBe(true);
    expect(list.contains("four")).toBe(false);

    list.remove("two");
    expect(list.contains("two")).toBe(false);
  });

  it("remove deletes the first occurrence of a value and reports whether it was present", async () => {
    const { list } = makeList<string>(new MemoryJournalStorage());
    list.add("one");
    list.add("two");
    list.add("three");

    const removed = list.remove("two");
    const removedAgain = list.remove("two");

    expect(removed).toBe(true);
    expect(removedAgain).toBe(false);
    expect(list.toArray()).toEqual(["one", "three"]);
  });

  it("insert adds a value at the given index, shifting later items", async () => {
    const { list } = makeList<string>(new MemoryJournalStorage());
    list.add("one");
    list.add("three");

    list.insert(1, "two");

    expect(list.length).toBe(3);
    expect(list.toArray()).toEqual(["one", "two", "three"]);
  });

  it("rebuilds from the log on a fresh activation, replaying insert and remove", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeList<string>(storage);
    a.list.add("one");
    a.list.add("three");
    a.list.insert(1, "two");
    a.list.remove("one");
    await a.manager.writeState();

    const b = makeList<string>(storage);
    await b.manager.replay();
    expect(b.list.toArray()).toEqual(["two", "three"]);
  });

  it("rebuilds from the log on a fresh activation (index ops replay in order)", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeList<number>(storage);
    a.list.add(1);
    a.list.add(2);
    a.list.add(3);
    a.list.removeAt(0); // [2,3]
    a.list.set(0, 20); // [20,3]
    a.list.add(4); // [20,3,4]
    await a.manager.writeState();

    const b = makeList<number>(storage);
    await b.manager.replay();
    expect(b.list.toArray()).toEqual([20, 3, 4]);
  });
});
