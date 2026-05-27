import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import { DurableDictionaryImpl } from "@tsva/journaling/durable-dictionary-impl";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@tsva/journaling/state-machine-manager-impl";

const id = new GrainId("Catalog", "k1");

function makeDict<K, V>(storage: MemoryJournalStorage) {
  const manager = new StateMachineManagerImpl("journal", id, storage);
  const dict = new DurableDictionaryImpl<K, V>("d", manager);
  manager.register(dict);
  return { manager, dict };
}

describe("DurableDictionaryImpl", () => {
  it("tracks set/delete/clear in memory", async () => {
    const { dict } = makeDict<string, number>(new MemoryJournalStorage());
    await dict.set("a", 1);
    await dict.set("b", 2);
    expect(dict.size).toBe(2);
    expect(dict.get("a")).toBe(1);
    expect(await dict.delete("a")).toBe(true);
    expect(await dict.delete("missing")).toBe(false);
    expect(dict.has("a")).toBe(false);
    await dict.clear();
    expect(dict.size).toBe(0);
  });

  it("rebuilds from the log on a fresh activation", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeDict<string, number>(storage);
    await a.dict.set("a", 1);
    await a.dict.set("b", 2);
    await a.dict.set("a", 10);
    await a.dict.delete("b");

    const b = makeDict<string, number>(storage);
    await b.manager.replay();
    expect([...b.dict.entries()]).toEqual([["a", 10]]);
  });

  it("replays a clear followed by later sets", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeDict<string, number>(storage);
    await a.dict.set("x", 1);
    await a.dict.clear();
    await a.dict.set("y", 2);

    const b = makeDict<string, number>(storage);
    await b.manager.replay();
    expect([...b.dict.entries()]).toEqual([["y", 2]]);
  });
});
