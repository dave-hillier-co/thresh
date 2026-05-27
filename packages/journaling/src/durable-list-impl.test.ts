import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import { DurableListImpl } from "@tsva/journaling/durable-list-impl";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@tsva/journaling/state-machine-manager-impl";

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
    await list.add("a");
    await list.add("b");
    await list.add("c");
    await list.set(1, "B");
    await list.removeAt(0);
    expect(list.toArray()).toEqual(["B", "c"]);
    expect([...list]).toEqual(["B", "c"]);
    await list.clear();
    expect(list.length).toBe(0);
  });

  it("rebuilds from the log on a fresh activation (index ops replay in order)", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeList<number>(storage);
    await a.list.add(1);
    await a.list.add(2);
    await a.list.add(3);
    await a.list.removeAt(0); // [2,3]
    await a.list.set(0, 20); // [20,3]
    await a.list.add(4); // [20,3,4]

    const b = makeList<number>(storage);
    await b.manager.replay();
    expect(b.list.toArray()).toEqual([20, 3, 4]);
  });
});
