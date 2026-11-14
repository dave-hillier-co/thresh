import { GrainId } from "@thresh/core/grain-id";
import { JournaledGrain, type CustomStorageInterface } from "@thresh/core/journaled-grain";
import { describe, expect, it } from "vitest";

import { bindJournaledGrain } from "./journaled-grain-binder";
import { JournalStorageRegistry } from "./journal-storage-registry";
import { MemoryJournalStorage } from "./memory-journal-storage";

type State = { readonly count: number };
type Event = { readonly amount: number };

class Counter extends JournaledGrain<State, Event> {
  initialState(): State {
    return { count: 0 };
  }
  transitionState(state: State, event: Event): State {
    return { count: state.count + event.amount };
  }
  tryAdd(amount: number): Promise<boolean> {
    return this.raiseConditionalEvent({ amount });
  }
  get confirmed(): State {
    return this.state;
  }
  get confirmedVersionNumber(): number {
    return this.version;
  }
}

/** A grain that owns its log in a shared array, as a custom-storage host does. */
class CustomCounter extends Counter implements CustomStorageInterface<State, Event> {
  constructor(private readonly log: Event[]) {
    super();
  }
  readStateFromStorage(): Promise<{ version: number; state: State }> {
    const state = this.log.reduce((s, e) => this.transitionState(s, e), this.initialState());
    return Promise.resolve({ version: this.log.length, state });
  }
  applyUpdatesToStorage(updates: readonly Event[], expectedVersion: number): Promise<boolean> {
    if (this.log.length !== expectedVersion) return Promise.resolve(false);
    this.log.push(...updates);
    return Promise.resolve(true);
  }
  clearStoredState(): Promise<void> {
    this.log.length = 0;
    return Promise.resolve();
  }
}

const id = new GrainId("Counter", "contract");

/** Each factory yields a function that activates a fresh incarnation over the same backing log. */
const adaptors: ReadonlyArray<{ name: string; backing: () => () => Promise<Counter> }> = [
  {
    name: "journal substrate adaptor",
    backing: () => {
      const storage = new MemoryJournalStorage();
      return async () => {
        const grain = new Counter();
        await bindJournaledGrain(grain, id, new JournalStorageRegistry().add("default", storage));
        return grain;
      };
    },
  },
  {
    name: "custom-storage adaptor",
    backing: () => {
      const log: Event[] = [];
      return async () => {
        const grain = new CustomCounter(log);
        await bindJournaledGrain(grain, id, new JournalStorageRegistry());
        return grain;
      };
    },
  },
];

describe.each(adaptors)("LogViewAdaptor.tryAppend contract: $name", ({ backing }) => {
  it("after a lost race, reports false once, then the confirmed view reflects storage", async () => {
    const activate = backing();
    const stale = await activate();
    expect(await stale.tryAdd(1)).toBe(true);

    const other = await activate();
    expect(await other.tryAdd(10)).toBe(true);

    expect(await stale.tryAdd(100)).toBe(false);

    const truth = await activate();
    expect(truth.confirmed).toEqual({ count: 11 });
    expect(stale.confirmed).toEqual(truth.confirmed);
    expect(stale.confirmedVersionNumber).toBe(truth.confirmedVersionNumber);
  });

  it("a retry computed from the refreshed view succeeds on top of the foreign write", async () => {
    const activate = backing();
    const stale = await activate();
    expect(await stale.tryAdd(1)).toBe(true);
    expect(await (await activate()).tryAdd(10)).toBe(true);

    expect(await stale.tryAdd(100)).toBe(false);
    expect(await stale.tryAdd(100)).toBe(true);

    expect((await activate()).confirmed).toEqual({ count: 111 });
    expect(stale.confirmed).toEqual({ count: 111 });
  });
});
