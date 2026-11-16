import { writeDurableState } from "@thresh/core/durable-state-metadata";
import { describe, expect, it } from "vitest";
import { durableState } from "@thresh/core/decorators";
import type { DurableValue } from "@thresh/core/durable-state";
import { GrainId } from "@thresh/core/grain-id";
import { JournaledGrain, type CustomStorageInterface } from "@thresh/core/journaled-grain";
import { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { bindJournalFacets } from "@thresh/journaling/journal-facets-binder";

type State = { readonly total: number };
type Event = { readonly add: number };

const id = new GrainId("Facets", "f1");

/** A custom-storage JournaledGrain that also has a journal-substrate `@durableState` field. */
class CustomStorageWithLabel
  extends JournaledGrain<State, Event>
  implements CustomStorageInterface<State, Event>
{
  @durableState("label")
  labelCell!: DurableValue<string>;

  log: Event[] = [];

  initialState(): State {
    return { total: 0 };
  }

  transitionState(state: State, event: Event): State {
    return { total: state.total + event.add };
  }

  readStateFromStorage(): Promise<{ version: number; state: State }> {
    return Promise.resolve({
      version: this.log.length,
      state: this.log.reduce((s, e) => this.transitionState(s, e), this.initialState()),
    });
  }

  applyUpdatesToStorage(updates: readonly Event[], expectedVersion: number): Promise<boolean> {
    if (this.log.length !== expectedVersion) return Promise.resolve(false);
    this.log = [...this.log, ...updates];
    return Promise.resolve(true);
  }

  clearStoredState(): Promise<void> {
    this.log = [];
    return Promise.resolve();
  }

  async add(n: number): Promise<void> {
    this.raiseEvent({ add: n });
    await this.confirmEvents();
  }

  get total(): number {
    return this.state.total;
  }
}

/** A substrate JournaledGrain whose durable field names a non-default journal provider. */
class OtherProviderLabel extends JournaledGrain<State, Event> {
  @durableState("label", { provider: "other" })
  labelCell!: DurableValue<string>;

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

  get total(): number {
    return this.state.total;
  }
}

describe("bindJournalFacets", () => {
  it("still binds the durable fields of a custom-storage JournaledGrain", async () => {
    const storage = new MemoryJournalStorage();
    const registry = new JournalStorageRegistry().add("default", storage);

    const grain = new CustomStorageWithLabel();
    await bindJournalFacets(grain, id, registry);
    grain.labelCell.set("alpha");
    await writeDurableState(grain);
    await grain.add(3);

    const again = new CustomStorageWithLabel();
    again.log = grain.log;
    await bindJournalFacets(again, id, registry);
    expect(again.labelCell.value).toBe("alpha");
    expect(again.total).toBe(3);
  });

  it("keeps each facet on its own provider when they name different journal stores", async () => {
    const defaultStorage = new MemoryJournalStorage();
    const otherStorage = new MemoryJournalStorage();
    const registry = new JournalStorageRegistry()
      .add("default", defaultStorage)
      .add("other", otherStorage);

    const grain = new OtherProviderLabel();
    await bindJournalFacets(grain, id, registry);
    await grain.add(2);
    grain.labelCell.set("beta");
    await writeDurableState(grain);

    // The JournaledGrain log stays on the default provider (where it always
    // lived, so existing history is still found) and the durable field on its
    // declared provider.
    expect((await defaultStorage.read("journal", id)).entries).toHaveLength(1);
    expect((await otherStorage.read("journal", id)).entries).toHaveLength(1);

    const again = new OtherProviderLabel();
    await bindJournalFacets(again, id, registry);
    expect(again.total).toBe(2);
    expect(again.labelCell.value).toBe("beta");
  });

  it("runs one manager over a log reached through two differently named providers on one backing", async () => {
    const defaultStorage = new MemoryJournalStorage();
    // A second provider name over the SAME backing store: a distinct provider object that
    // reads and writes the same logs (as two Postgres providers over one pool and tables do).
    const otherStorage = new MemoryJournalStorage({ sharing: defaultStorage });
    const registry = new JournalStorageRegistry()
      .add("default", defaultStorage)
      .add("other", otherStorage);

    const grain = new OtherProviderLabel();
    await bindJournalFacets(grain, id, registry);
    await grain.add(2);
    grain.labelCell.set("beta");
    await writeDurableState(grain);
    await grain.add(5);

    const again = new OtherProviderLabel();
    await bindJournalFacets(again, id, registry);
    expect(again.total).toBe(7);
    expect(again.labelCell.value).toBe("beta");
  });
});
