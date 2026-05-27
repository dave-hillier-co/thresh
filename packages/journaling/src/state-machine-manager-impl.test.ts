import { describe, expect, it } from "vitest";
import { InconsistentStateError } from "@tsva/core/errors";
import { GrainId } from "@tsva/core/grain-id";
import { DurableValueImpl } from "@tsva/journaling/durable-value-impl";
import { DurableDictionaryImpl } from "@tsva/journaling/durable-dictionary-impl";
import { DurableListImpl } from "@tsva/journaling/durable-list-impl";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@tsva/journaling/state-machine-manager-impl";

const id = new GrainId("Agg", "g1");

async function frameKinds(storage: MemoryJournalStorage): Promise<string[]> {
  const segment = await storage.read("journal", id);
  return segment.entries.map((e) => JSON.parse(e).k as string);
}

describe("StateMachineManagerImpl", () => {
  it("rejects a duplicate machine name", () => {
    const manager = new StateMachineManagerImpl("journal", id, new MemoryJournalStorage());
    manager.register(new DurableValueImpl("dup", manager));
    expect(() => manager.register(new DurableValueImpl("dup", manager))).toThrow(/duplicate/);
  });

  it("snapshots and truncates the log past the threshold, and replays correctly", async () => {
    const storage = new MemoryJournalStorage();
    const manager = new StateMachineManagerImpl("journal", id, storage, { snapshotThreshold: 4 });
    const value = new DurableValueImpl<number>("v", manager);
    manager.register(value);
    await manager.replay();

    // 4 appends crosses the threshold and triggers a compaction.
    await value.set(1);
    await value.set(2);
    await value.set(3);
    await value.set(4);

    const kinds = await frameKinds(storage);
    expect(kinds).toEqual(["snap"]); // log truncated to one snapshot frame
    const { version } = await storage.read("journal", id);
    expect(version).toBe(5); // 4 appends + 1 replace

    // Further mutations after the snapshot, then a fresh activation replays it all.
    await value.set(5);
    const fresh = new StateMachineManagerImpl("journal", id, storage, { snapshotThreshold: 4 });
    const replayed = new DurableValueImpl<number>("v", fresh);
    fresh.register(replayed);
    await fresh.replay();
    expect(replayed.value).toBe(5);
  });

  it("shares one log across multiple structures and replays each", async () => {
    const storage = new MemoryJournalStorage();
    const manager = new StateMachineManagerImpl("journal", id, storage, {
      snapshotThreshold: 1000,
    });
    const value = new DurableValueImpl<string>("v", manager);
    const dict = new DurableDictionaryImpl<string, number>("d", manager);
    const list = new DurableListImpl<number>("l", manager);
    manager.register(value);
    manager.register(dict);
    manager.register(list);
    await manager.replay();

    await value.set("hello");
    await dict.set("a", 1);
    await list.add(10);
    await dict.set("b", 2);
    await list.add(20);

    // All five mutations live in the single shared log.
    expect((await storage.read("journal", id)).entries).toHaveLength(5);

    const fresh = new StateMachineManagerImpl("journal", id, storage, { snapshotThreshold: 1000 });
    const v2 = new DurableValueImpl<string>("v", fresh);
    const d2 = new DurableDictionaryImpl<string, number>("d", fresh);
    const l2 = new DurableListImpl<number>("l", fresh);
    fresh.register(v2);
    fresh.register(d2);
    fresh.register(l2);
    await fresh.replay();

    expect(v2.value).toBe("hello");
    expect([...d2.entries()]).toEqual([
      ["a", 1],
      ["b", 2],
    ]);
    expect(l2.toArray()).toEqual([10, 20]);
  });

  it("fences out a stale incarnation via the version CAS, leaving its memory untouched", async () => {
    const storage = new MemoryJournalStorage();
    const a = new StateMachineManagerImpl("journal", id, storage);
    const av = new DurableValueImpl<number>("v", a);
    a.register(av);
    await a.replay();

    const b = new StateMachineManagerImpl("journal", id, storage);
    const bv = new DurableValueImpl<number>("v", b);
    b.register(bv);
    await b.replay(); // both at the empty version

    await av.set(1); // A wins the append

    await expect(bv.set(99)).rejects.toBeInstanceOf(InconsistentStateError);
    expect(bv.value).toBeUndefined(); // append-before-apply: B's memory unchanged
  });
});
