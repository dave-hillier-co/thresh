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

  describe("retirement of unregistered structures", () => {
    it("keeps an unregistered structure's data through one compaction, purging it on the next", async () => {
      const storage = new MemoryJournalStorage();

      // Two structures registered; "retire" will stop being registered from
      // the next activation onward (simulating a field removed across a deploy).
      const gen1 = new StateMachineManagerImpl("journal", id, storage);
      const keep1 = new DurableDictionaryImpl<string, number>("keep", gen1);
      const retire1 = new DurableDictionaryImpl<string, number>("retire", gen1);
      gen1.register(keep1);
      gen1.register(retire1);
      await gen1.replay();
      await keep1.set("a", 1);
      await retire1.set("b", 1);

      // Next activation: only "keep" is registered. "retire" becomes an orphan.
      const gen2 = new StateMachineManagerImpl("journal", id, storage);
      const keep2 = new DurableDictionaryImpl<string, number>("keep", gen2);
      gen2.register(keep2);
      await gen2.replay();
      expect(keep2.get("a")).toBe(1);

      await keep2.set("a", 10);
      await gen2.compact(); // 1st compaction while orphaned: within the default grace of 2, kept

      const gen3 = new StateMachineManagerImpl("journal", id, storage);
      const keep3 = new DurableDictionaryImpl<string, number>("keep", gen3);
      const retire3 = new DurableDictionaryImpl<string, number>("retire", gen3);
      gen3.register(keep3);
      gen3.register(retire3);
      await gen3.replay();
      // Proves "retire"'s data survived gen2's compaction even though gen2
      // never had it registered.
      expect(retire3.get("b")).toBe(1);
      expect(keep3.get("a")).toBe(10);
    });

    it("purges an unregistered structure for good once its grace period elapses", async () => {
      const storage = new MemoryJournalStorage();

      const gen1 = new StateMachineManagerImpl("journal", id, storage);
      const keep1 = new DurableDictionaryImpl<string, number>("keep", gen1);
      const retire1 = new DurableDictionaryImpl<string, number>("retire", gen1);
      gen1.register(keep1);
      gen1.register(retire1);
      await gen1.replay();
      await retire1.set("b", 1);

      // Two activations in a row without "retire" registered, each compacting
      // once: 1st compaction keeps it (grace = 2), 2nd purges it for good.
      for (let i = 0; i < 2; i += 1) {
        const gen = new StateMachineManagerImpl("journal", id, storage);
        const keep = new DurableDictionaryImpl<string, number>("keep", gen);
        gen.register(keep);
        await gen.replay();
        await keep.set("a", i);
        await gen.compact();
      }

      const genFinal = new StateMachineManagerImpl("journal", id, storage);
      const keepFinal = new DurableDictionaryImpl<string, number>("keep", genFinal);
      const retireFinal = new DurableDictionaryImpl<string, number>("retire", genFinal);
      genFinal.register(keepFinal);
      genFinal.register(retireFinal);
      await genFinal.replay();

      expect(keepFinal.get("a")).toBe(1);
      // "retire" comes back empty: its data was purged, not resurrected.
      expect(retireFinal.has("b")).toBe(false);
    });

    it("resurrects a re-registered structure immediately, before its grace period elapses", async () => {
      const storage = new MemoryJournalStorage();

      const gen1 = new StateMachineManagerImpl("journal", id, storage);
      const keep1 = new DurableDictionaryImpl<string, number>("keep", gen1);
      const retire1 = new DurableDictionaryImpl<string, number>("retire", gen1);
      gen1.register(keep1);
      gen1.register(retire1);
      await gen1.replay();
      await retire1.set("b", 1);

      // "retire" goes unregistered for one activation and one compaction...
      const gen2 = new StateMachineManagerImpl("journal", id, storage);
      const keep2 = new DurableDictionaryImpl<string, number>("keep", gen2);
      gen2.register(keep2);
      await gen2.replay();
      await keep2.set("a", 1);
      await gen2.compact();

      // ...then comes back before the grace period (2 compactions) elapses.
      const gen3 = new StateMachineManagerImpl("journal", id, storage);
      const keep3 = new DurableDictionaryImpl<string, number>("keep", gen3);
      const retire3 = new DurableDictionaryImpl<string, number>("retire", gen3);
      gen3.register(keep3);
      gen3.register(retire3);
      await gen3.replay();
      expect(retire3.get("b")).toBe(1); // resurrected with its original data

      // Even after another compaction here, it is registered so nothing purges it.
      await gen3.compact();

      const gen4 = new StateMachineManagerImpl("journal", id, storage);
      const keep4 = new DurableDictionaryImpl<string, number>("keep", gen4);
      const retire4 = new DurableDictionaryImpl<string, number>("retire", gen4);
      gen4.register(keep4);
      gen4.register(retire4);
      await gen4.replay();
      expect(retire4.get("b")).toBe(1); // still there: it was never purged
    });

    it("resets the grace period if a resurrected structure is removed again", async () => {
      const storage = new MemoryJournalStorage();

      const gen1 = new StateMachineManagerImpl("journal", id, storage);
      const keep1 = new DurableDictionaryImpl<string, number>("keep", gen1);
      const retire1 = new DurableDictionaryImpl<string, number>("retire", gen1);
      gen1.register(keep1);
      gen1.register(retire1);
      await gen1.replay();
      await retire1.set("b", 1);

      // Orphaned for one compaction (grace = 2, so 1 of 2 spent)...
      const gen2 = new StateMachineManagerImpl("journal", id, storage);
      const keep2 = new DurableDictionaryImpl<string, number>("keep", gen2);
      gen2.register(keep2);
      await gen2.replay();
      await keep2.set("a", 1);
      await gen2.compact();

      // ...resurrected (writes a normal snapshot for it, clearing the tracker)...
      const gen3 = new StateMachineManagerImpl("journal", id, storage);
      const keep3 = new DurableDictionaryImpl<string, number>("keep", gen3);
      const retire3 = new DurableDictionaryImpl<string, number>("retire", gen3);
      gen3.register(keep3);
      gen3.register(retire3);
      await gen3.replay();
      await keep3.set("a", 2);
      await gen3.compact();

      // ...then orphaned again. If the grace period had carried over from
      // before the resurrection, a single further compaction would purge it;
      // it should instead take a fresh 2-compaction grace period.
      const gen4 = new StateMachineManagerImpl("journal", id, storage);
      const keep4 = new DurableDictionaryImpl<string, number>("keep", gen4);
      gen4.register(keep4);
      await gen4.replay();
      await keep4.set("a", 3);
      await gen4.compact(); // 1st compaction of the fresh grace period: kept

      const gen5 = new StateMachineManagerImpl("journal", id, storage);
      const keep5 = new DurableDictionaryImpl<string, number>("keep", gen5);
      const retire5 = new DurableDictionaryImpl<string, number>("retire", gen5);
      gen5.register(keep5);
      gen5.register(retire5);
      await gen5.replay();
      expect(retire5.get("b")).toBe(1); // still alive: only 1 of 2 compactions spent
    });

    it("supports a configurable grace period", async () => {
      const storage = new MemoryJournalStorage();

      const gen1 = new StateMachineManagerImpl("journal", id, storage, {
        retirementGraceCompactions: 1,
      });
      const keep1 = new DurableDictionaryImpl<string, number>("keep", gen1);
      const retire1 = new DurableDictionaryImpl<string, number>("retire", gen1);
      gen1.register(keep1);
      gen1.register(retire1);
      await gen1.replay();
      await retire1.set("b", 1);

      // A grace period of 1 means the very first compaction while unregistered purges it.
      const gen2 = new StateMachineManagerImpl("journal", id, storage, {
        retirementGraceCompactions: 1,
      });
      const keep2 = new DurableDictionaryImpl<string, number>("keep", gen2);
      gen2.register(keep2);
      await gen2.replay();
      await keep2.set("a", 1);
      await gen2.compact();

      const gen3 = new StateMachineManagerImpl("journal", id, storage, {
        retirementGraceCompactions: 1,
      });
      const keep3 = new DurableDictionaryImpl<string, number>("keep", gen3);
      const retire3 = new DurableDictionaryImpl<string, number>("retire", gen3);
      gen3.register(keep3);
      gen3.register(retire3);
      await gen3.replay();
      expect(retire3.has("b")).toBe(false);
    });
  });
});
