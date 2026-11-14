import { durableState } from "@thresh/core/decorators";
import type { DurableValue } from "@thresh/core/durable-state";
import { GrainId } from "@thresh/core/grain-id";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import { describe, expect, it } from "vitest";

import { bindDurableStates } from "./durable-state-activator";
import { bindJournaledGrain } from "./journaled-grain-binder";
import { bindJournalFacets } from "./journal-facets-binder";
import { JournalStorageRegistry } from "./journal-storage-registry";
import { MemoryJournalStorage } from "./memory-journal-storage";

type Event = { readonly n: number };

class Ledger extends JournaledGrain<{ sum: number }, Event> {
  transitions = 0;
  initialState() {
    return { sum: 0 };
  }
  transitionState(state: { sum: number }, event: Event) {
    this.transitions += 1;
    return { sum: state.sum + event.n };
  }
  async add(n: number): Promise<void> {
    this.raiseEvent({ n });
    await this.confirmEvents();
  }
  history(from: number, to: number): readonly Event[] {
    return this.retrieveConfirmedEvents(from, to);
  }
  get sum(): number {
    return this.state.sum;
  }
  get head(): number {
    return this.version;
  }
}

class LedgerWithLabel extends Ledger {
  @durableState("label")
  label!: DurableValue<string>;
}

class OnlyCollections {
  @durableState("counter")
  counter!: DurableValue<number>;
}

const id = new GrainId("Ledger", "l1");

function registryOver(storage: MemoryJournalStorage): JournalStorageRegistry {
  return new JournalStorageRegistry().add("default", storage);
}

const retain = { retainEvents: true } as const;

async function raiseMany(grain: Ledger, count: number): Promise<void> {
  for (let i = 1; i <= count; i++) await grain.add(i);
}

describe("JournaledGrain history across compaction", () => {
  it("keeps its full event range after compaction when retention is switched on", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new Ledger();
    await bindJournaledGrain(grain, id, registryOver(storage), retain);
    await raiseMany(grain, 250);

    const history = grain.history(0, 250);
    expect(history).toHaveLength(250);
    expect(history[0]).toEqual({ n: 1 });
    expect(history[249]).toEqual({ n: 250 });
    expect(grain.head).toBe(250);
  });

  it("keeps its full event range after re-activation", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new Ledger();
    await bindJournaledGrain(grain, id, registryOver(storage), retain);
    await raiseMany(grain, 250);

    const again = new Ledger();
    await bindJournaledGrain(again, id, registryOver(storage), retain);
    expect(again.head).toBe(250);
    expect(again.sum).toBe(grain.sum);
    expect(again.history(0, 250)).toEqual(grain.history(0, 250));
    // Events raised after re-activation extend the same history.
    await again.add(1000);
    expect(again.history(249, 251)).toEqual([{ n: 250 }, { n: 1000 }]);
  });

  it("still re-activates from the snapshot rather than replaying every event", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new Ledger();
    await bindJournaledGrain(grain, id, registryOver(storage), { snapshotThreshold: 20, ...retain });
    await raiseMany(grain, 100);

    const again = new Ledger();
    await bindJournaledGrain(again, id, registryOver(storage), { snapshotThreshold: 20, ...retain });
    expect(again.sum).toBe(grain.sum);
    expect(again.head).toBe(100);
    // Only the events after the latest snapshot are folded through transitionState.
    expect(again.transitions).toBeLessThanOrEqual(20);
  });

  it("keeps history alongside durable fields sharing the log", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new LedgerWithLabel();
    await bindJournalFacets(grain, id, registryOver(storage), { snapshotThreshold: 10, ...retain });
    for (let i = 1; i <= 40; i++) {
      await grain.add(i);
      await grain.label.set(`label-${i}`);
    }
    const again = new LedgerWithLabel();
    await bindJournalFacets(again, id, registryOver(storage), { snapshotThreshold: 10, ...retain });
    expect(again.history(0, 40)).toHaveLength(40);
    expect(again.label.value).toBe("label-40");
  });

  it("truncates by default: the log shrinks and nothing below the snapshot is held", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new Ledger();
    await bindJournaledGrain(grain, id, registryOver(storage), { snapshotThreshold: 10 });
    await raiseMany(grain, 50);

    const { entries } = await storage.read("journal", id);
    expect(entries.length).toBeLessThan(10);
    expect(grain.head).toBe(50);
    // Only the tail since the snapshot is retrievable.
    expect(() => grain.history(0, 50)).toThrow(/retainEvents/);
    expect(() => grain.history(49, 50)).not.toThrow();
  });

  it("with defaults, a re-activation holds no history below the snapshot either", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new Ledger();
    await bindJournaledGrain(grain, id, registryOver(storage), { snapshotThreshold: 10 });
    await raiseMany(grain, 50);
    const again = new Ledger();
    await bindJournaledGrain(again, id, registryOver(storage), { snapshotThreshold: 10 });
    expect(again.head).toBe(50);
    expect(() => again.history(0, 50)).toThrow(/retainEvents/);
  });
});

describe("durable collections without a JournaledGrain", () => {
  it("still truncates the log at the threshold", async () => {
    const storage = new MemoryJournalStorage();
    const grain = new OnlyCollections();
    await bindDurableStates(grain, id, registryOver(storage), { snapshotThreshold: 10 });
    for (let i = 0; i < 95; i++) await grain.counter.set(i);

    const { entries } = await storage.read("journal", id);
    expect(entries.length).toBeLessThan(10);

    const again = new OnlyCollections();
    await bindDurableStates(again, id, registryOver(storage), { snapshotThreshold: 10 });
    expect(again.counter.value).toBe(94);
  });
});
