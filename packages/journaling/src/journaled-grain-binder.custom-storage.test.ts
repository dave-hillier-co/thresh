import { GrainId } from "@thresh/core/grain-id";
import { JournaledGrain, type CustomStorageInterface } from "@thresh/core/journaled-grain";
import { describe, expect, it } from "vitest";

import { JournalStorageRegistry } from "./journal-storage-registry";
import { MemoryJournalStorage } from "./memory-journal-storage";
import { bindJournaledGrain } from "./journaled-grain-binder";

type State = { readonly total: number };
type Event = { readonly add: number };

/** A shared "database" the grain reads and appends to, standing in for real storage. */
const store = new Map<string, Event[]>();
/** When set, `applyUpdatesToStorage` waits on it: a write held at its storage await. */
let appendGate: Promise<void> | undefined;

class CountingGrain
  extends JournaledGrain<State, Event>
  implements CustomStorageInterface<State, Event>
{
  key = "counter";

  initialState(): State {
    return { total: 0 };
  }

  transitionState(state: State, event: Event): State {
    return { total: state.total + event.add };
  }

  private get log(): Event[] {
    return store.get(this.key) ?? [];
  }

  readStateFromStorage(): Promise<{ version: number; state: State }> {
    const log = this.log;
    return Promise.resolve({
      version: log.length,
      state: log.reduce((s, e) => this.transitionState(s, e), this.initialState()),
    });
  }

  async applyUpdatesToStorage(
    updates: readonly Event[],
    expectedVersion: number,
  ): Promise<boolean> {
    if (appendGate !== undefined) await appendGate;
    const log = this.log;
    if (log.length !== expectedVersion) return false;
    store.set(this.key, [...log, ...updates]);
    return true;
  }

  clearStoredState(): Promise<void> {
    store.delete(this.key);
    return Promise.resolve();
  }

  async add(n: number): Promise<void> {
    this.raiseEvent({ add: n });
    await this.confirmEvents();
  }

  async addBatch(events: readonly Event[]): Promise<void> {
    for (const event of events) this.raiseEvent(event);
    await this.confirmEvents();
  }

  get confirmed(): State {
    return this.state;
  }

  get confirmedVersion(): number {
    return this.version;
  }
}

/** A plain journaled grain, to prove the substrate path is unchanged. */
class PlainGrain extends JournaledGrain<State, Event> {
  initialState(): State {
    return { total: 0 };
  }
  transitionState(state: State, event: Event): State {
    return { total: state.total + event.add };
  }
  async add(n: number): Promise<void> {
    this.raiseEvent({ add: n });
    await this.confirmEvents();
  }
  get confirmed(): State {
    return this.state;
  }
}

function registry(): JournalStorageRegistry {
  return new JournalStorageRegistry().add("default", new MemoryJournalStorage());
}

describe("bindJournaledGrain with a custom-storage host", () => {
  it("routes a custom-storage grain to its own storage, not the journal substrate", async () => {
    store.clear();
    const grain = new CountingGrain();
    await bindJournaledGrain(grain, new GrainId("Counting", "counter"), registry());

    await grain.add(3);
    await grain.add(4);

    expect(grain.confirmed).toEqual({ total: 7 });
    expect(grain.confirmedVersion).toBe(2);
    // The grain's own store holds the log; the journal substrate was never written.
    expect(store.get("counter")).toEqual([{ add: 3 }, { add: 4 }]);
  });

  it("replays from the grain's own storage on reactivation", async () => {
    store.clear();
    store.set("counter", [{ add: 10 }, { add: 5 }]);

    const revived = new CountingGrain();
    await bindJournaledGrain(revived, new GrainId("Counting", "counter"), registry());

    expect(revived.confirmed).toEqual({ total: 15 });
    expect(revived.confirmedVersion).toBe(2);
  });

  it("holds the confirmed view at the pre-write state for the whole of a write's storage await (#87)", async () => {
    // What a reader interleaved into a write's await sees: the confirmed view
    // (`state`/`version`) moves in one step once the append is durable, never
    // through a partially-applied batch. BeneDB's DatastoreGrain reads rely on it.
    store.clear();
    const grain = new CountingGrain();
    await bindJournaledGrain(grain, new GrainId("Counting", "batch"), registry());
    let open!: () => void;
    appendGate = new Promise<void>((resolve) => (open = resolve));
    try {
      const write = grain.addBatch([{ add: 1 }, { add: 2 }]);
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(grain.confirmed).toEqual({ total: 0 });
      expect(grain.confirmedVersion).toBe(0);

      open();
      await write;
      expect(grain.confirmed).toEqual({ total: 3 });
      expect(grain.confirmedVersion).toBe(2);
    } finally {
      appendGate = undefined;
    }
  });

  it("leaves a plain journaled grain on the substrate path", async () => {
    const grain = new PlainGrain();
    await bindJournaledGrain(grain, new GrainId("Plain", "p1"), registry());

    await grain.add(2);

    expect(grain.confirmed).toEqual({ total: 2 });
  });
});
