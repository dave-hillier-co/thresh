import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { bindJournaledGrain } from "@thresh/journaling/journaled-grain-binder";

interface CountState {
  count: number;
}

type CountEvent = { kind: "add"; amount: number } | { kind: "reset" };

class CounterGrain extends JournaledGrain<CountState, CountEvent> {
  initialState(): CountState {
    return { count: 0 };
  }

  transitionState(state: CountState, event: CountEvent): CountState {
    if (event.kind === "add") return { count: state.count + event.amount };
    return { count: 0 };
  }

  add(amount: number): void {
    this.raiseEvent({ kind: "add", amount });
  }

  reset(): void {
    this.raiseEvent({ kind: "reset" });
  }

  confirm(): Promise<void> {
    return this.confirmEvents();
  }

  clear(): Promise<void> {
    return this.clearLog();
  }

  tentative(): CountState {
    return this.tentativeState;
  }

  confirmed(): CountState {
    return this.state;
  }

  confirmedVersion(): number {
    return this.version;
  }
}

const id = new GrainId("Counter", "c1");

async function makeGrain(storage: MemoryJournalStorage): Promise<CounterGrain> {
  const grain = new CounterGrain();
  await bindJournaledGrain(grain, id, new JournalStorageRegistry().add("default", storage));
  return grain;
}

describe("JournaledGrain log-consistency protocol", () => {
  it("makes a raised event visible tentatively but not confirmed until confirmEvents", async () => {
    const grain = await makeGrain(new MemoryJournalStorage());

    expect(grain.tentative()).toEqual({ count: 0 });
    expect(grain.confirmed()).toEqual({ count: 0 });
    expect(grain.confirmedVersion()).toBe(0);

    grain.add(3);

    // Visible in the tentative view immediately, synchronously.
    expect(grain.tentative()).toEqual({ count: 3 });
    // Not yet reflected in the confirmed view/version.
    expect(grain.confirmed()).toEqual({ count: 0 });
    expect(grain.confirmedVersion()).toBe(0);

    await grain.confirm();

    // Confirmation persists the event and promotes it into the confirmed view.
    expect(grain.confirmed()).toEqual({ count: 3 });
    expect(grain.confirmedVersion()).toBe(1);
    expect(grain.tentative()).toEqual({ count: 3 });
  });

  it("folds multiple pending events into the tentative view before confirmation", async () => {
    const grain = await makeGrain(new MemoryJournalStorage());

    grain.add(1);
    grain.add(1);
    grain.add(1);
    expect(grain.tentative()).toEqual({ count: 3 });
    expect(grain.confirmed()).toEqual({ count: 0 });

    await grain.confirm();
    expect(grain.confirmed()).toEqual({ count: 3 });
    expect(grain.confirmedVersion()).toBe(3); // version = number of confirmed events
  });

  it("replays the confirmed log on activation, surviving deactivation", async () => {
    const storage = new MemoryJournalStorage();

    const a = await makeGrain(storage);
    a.add(5);
    a.add(2);
    await a.confirm();
    expect(a.confirmed()).toEqual({ count: 7 });
    expect(a.confirmedVersion()).toBe(2);

    // A fresh activation of the "same" grain (new instance, same grain id,
    // same storage) must replay the durable log to rebuild confirmed state.
    const b = await makeGrain(storage);
    expect(b.confirmed()).toEqual({ count: 7 });
    expect(b.tentative()).toEqual({ count: 7 });
    expect(b.confirmedVersion()).toBe(2);
  });

  it("clearLog resets confirmed and tentative state and drops pending events", async () => {
    const storage = new MemoryJournalStorage();
    const grain = await makeGrain(storage);
    grain.add(10);
    await grain.confirm();
    grain.add(4); // pending, unconfirmed

    expect(grain.tentative()).toEqual({ count: 14 });

    await grain.clear();

    expect(grain.confirmed()).toEqual({ count: 0 });
    expect(grain.tentative()).toEqual({ count: 0 });
    expect(grain.confirmedVersion()).toBe(0);

    // Further writes after a clear behave as if the log started fresh.
    grain.add(1);
    await grain.confirm();
    expect(grain.confirmed()).toEqual({ count: 1 });
    expect(grain.confirmedVersion()).toBe(1);
  });

  it("retrieveConfirmedEvents returns the requested slice of the confirmed sequence", async () => {
    const grain = await makeGrain(new MemoryJournalStorage());
    grain.add(1);
    grain.add(2);
    grain.add(3);
    await grain.confirm();

    const events = grain["retrieveConfirmedEvents"](0, 3);
    expect(events).toEqual([
      { kind: "add", amount: 1 },
      { kind: "add", amount: 2 },
      { kind: "add", amount: 3 },
    ]);
    expect(grain["retrieveConfirmedEvents"](1, 2)).toEqual([{ kind: "add", amount: 2 }]);
  });
});
