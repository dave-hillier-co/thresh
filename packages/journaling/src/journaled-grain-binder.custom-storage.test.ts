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

  applyUpdatesToStorage(updates: readonly Event[], expectedVersion: number): Promise<boolean> {
    const log = this.log;
    if (log.length !== expectedVersion) return Promise.resolve(false);
    store.set(this.key, [...log, ...updates]);
    return Promise.resolve(true);
  }

  clearStoredState(): Promise<void> {
    store.delete(this.key);
    return Promise.resolve();
  }

  async add(n: number): Promise<void> {
    this.raiseEvent({ add: n });
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

  it("leaves a plain journaled grain on the substrate path", async () => {
    const grain = new PlainGrain();
    await bindJournaledGrain(grain, new GrainId("Plain", "p1"), registry());

    await grain.add(2);

    expect(grain.confirmed).toEqual({ total: 2 });
  });
});
