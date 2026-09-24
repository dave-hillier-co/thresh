import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainId as GrainIdType } from "@thresh/core/grain-id";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";
import { InconsistentStateError } from "@thresh/core/errors";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import { serializeValue } from "@thresh/core/value-codec";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { JournalStorageRegistry } from "@thresh/journaling/journal-storage-registry";
import { bindJournaledGrain } from "@thresh/journaling/journaled-grain-binder";

/** Wraps a `JournalStorage`, throwing on the Nth `append` call (1-indexed) instead of delegating. */
class FailingAppendStorage implements JournalStorage {
  private calls = 0;

  constructor(
    private readonly inner: JournalStorage,
    private readonly failOnCall: number,
    private readonly error: () => Error,
  ) {}

  read(logName: string, grainId: GrainIdType, signal?: AbortSignal): Promise<JournalSegment> {
    return this.inner.read(logName, grainId, signal);
  }

  append(
    logName: string,
    grainId: GrainIdType,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number> {
    this.calls += 1;
    if (this.calls === this.failOnCall) throw this.error();
    return this.inner.append(logName, grainId, entries, expectedVersion, signal);
  }

  replace(
    logName: string,
    grainId: GrainIdType,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number> {
    return this.inner.replace(logName, grainId, entries, expectedVersion, signal);
  }

  clear(logName: string, grainId: GrainIdType, signal?: AbortSignal): Promise<void> {
    return this.inner.clear(logName, grainId, signal);
  }
}

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

  tryAdd(amount: number): Promise<boolean> {
    return this.raiseConditionalEvent({ kind: "add", amount });
  }

  tentativeVer(): number {
    return this.tentativeVersion;
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

async function makeGrain(
  storage: MemoryJournalStorage,
  opts: { snapshotThreshold?: number } = {},
): Promise<CounterGrain> {
  const grain = new CounterGrain();
  await bindJournaledGrain(grain, id, new JournalStorageRegistry().add("default", storage), opts);
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

  it("keeps the un-appended remainder of a batch pending after a transient append failure (#95)", async () => {
    const memory = new MemoryJournalStorage();
    // Fail the second `append` call: the first raised event ("add 1") persists,
    // the second ("add 10") hits a transient storage error.
    const flaky = new FailingAppendStorage(memory, 2, () => new Error("transient storage blip"));
    const grain = new CounterGrain();
    await bindJournaledGrain(grain, id, new JournalStorageRegistry().add("default", flaky));

    grain.add(1);
    grain.add(10);
    await expect(grain.confirm()).rejects.toThrow("transient storage blip");

    // The first event persisted; the second must still be pending, not lost.
    expect(grain.confirmed()).toEqual({ count: 1 });
    expect(grain.confirmedVersion()).toBe(1);
    expect(grain.tentative()).toEqual({ count: 11 });

    // Retrying confirm (storage now healthy) must persist the surviving event.
    await grain.confirm();
    expect(grain.confirmed()).toEqual({ count: 11 });
    expect(grain.confirmedVersion()).toBe(2);
    expect(grain.tentative()).toEqual({ count: 11 });
  });

  it("keeps the version monotonic across a compaction (#96)", async () => {
    const storage = new MemoryJournalStorage();
    const grain = await makeGrain(storage, { snapshotThreshold: 5 });

    // 7 events crosses the threshold-5 compaction at least once.
    for (let i = 0; i < 7; i++) {
      grain.add(1);
      await grain.confirm();
    }
    expect(grain.confirmed()).toEqual({ count: 7 });
    expect(grain.confirmedVersion()).toBe(7);

    // Reactivate: replay must restore the compacted state AND its version,
    // not just count entries still in the (now-truncated) log.
    const reactivated = await makeGrain(storage, { snapshotThreshold: 5 });
    expect(reactivated.confirmed()).toEqual({ count: 7 });
    expect(reactivated.confirmedVersion()).toBe(7);

    // retrieveConfirmedEvents must behave sensibly (not throw) for a range
    // that is entirely within the confirmed history after compaction.
    expect(reactivated["retrieveConfirmedEvents"](7, 7)).toEqual([]);

    // A further event advances the version from where it left off, not from 0.
    reactivated.add(1);
    await reactivated.confirm();
    expect(reactivated.confirmed()).toEqual({ count: 8 });
    expect(reactivated.confirmedVersion()).toBe(8);
  });

  it("drops a conflicting conditional event from the tentative view too, not just the count", async () => {
    const memory = new MemoryJournalStorage();
    const conflicting = new FailingAppendStorage(
      memory,
      1,
      () => new InconsistentStateError("journal version conflict", undefined, undefined),
    );
    const grain = new CounterGrain();
    await bindJournaledGrain(grain, id, new JournalStorageRegistry().add("default", conflicting));

    expect(await grain.tryAdd(5)).toBe(false);

    // The event was not applied and will not be: the tentative view and
    // version must agree with the confirmed ones rather than still show it.
    expect(grain.confirmed()).toEqual({ count: 0 });
    expect(grain.tentative()).toEqual({ count: 0 });
    expect(grain.tentativeVer()).toBe(grain.confirmedVersion());
  });

  it("reads a snapshot frame written before the version was recorded in it", async () => {
    const storage = new MemoryJournalStorage();
    // A pre-#96 compaction frame: state only, no `v`.
    await storage.append(
      "journal",
      id,
      [serializeValue({ m: "journal", k: "snap", p: { t: "snap", s: { count: 4 } } })],
      undefined,
    );

    const grain = await makeGrain(storage);
    expect(grain.confirmed()).toEqual({ count: 4 });
    expect(grain.confirmedVersion()).toBe(0);

    grain.add(1);
    await grain.confirm();
    expect(grain.confirmedVersion()).toBe(1);
    expect(grain["retrieveConfirmedEvents"](0, 1)).toEqual([{ kind: "add", amount: 1 }]);
  });
});
