import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { StateHolder } from "@thresh/core/grain-storage";
import { sharesJournalBacking, type JournalStorage } from "@thresh/core/journal-storage";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import {
  FaultInjectingGrainStorage,
  FaultInjectingJournalStorage,
  InjectedStorageFault,
} from "@thresh/testing/fault-injecting-storage";

const ACCOUNT = new GrainId("Account", "a");
const OTHER = new GrainId("Ledger", "l");

const holder = <T>(value: T): StateHolder<T> => ({ value, exists: false });

async function stored<T>(storage: MemoryGrainStorage, id: GrainId, initial: T): Promise<T> {
  const h = holder(initial);
  await storage.read("state", id, h);
  return h.value;
}

/** Whether `promise` has settled after the microtask queue drains. */
async function settled(promise: Promise<unknown>): Promise<boolean> {
  let done = false;
  void promise.then(
    () => (done = true),
    () => (done = true),
  );
  for (let i = 0; i < 10; i++) await Promise.resolve();
  return done;
}

describe("FaultInjectingGrainStorage", () => {
  it("passes every operation through while no fault is set", async () => {
    const inner = new MemoryGrainStorage();
    const storage = new FaultInjectingGrainStorage(inner);
    await storage.write("state", ACCOUNT, holder({ balance: 5 }));
    expect(await stored(inner, ACCOUNT, {})).toEqual({ balance: 5 });
    const read = holder({});
    await storage.read("state", ACCOUNT, read);
    expect(read.value).toEqual({ balance: 5 });
  });

  it("fails the chosen operation the chosen number of times, and nothing else", async () => {
    const inner = new MemoryGrainStorage();
    const storage = new FaultInjectingGrainStorage(inner);
    storage.fail("write", { count: 1 });

    await expect(storage.write("state", ACCOUNT, holder({ balance: 1 }))).rejects.toBeInstanceOf(
      InjectedStorageFault,
    );
    expect(await stored(inner, ACCOUNT, "absent")).toBe("absent");
    await storage.read("state", ACCOUNT, holder({}));
    await storage.write("state", ACCOUNT, holder({ balance: 2 }));
    expect(await stored(inner, ACCOUNT, {})).toEqual({ balance: 2 });
    expect(storage.injected).toBe(1);
  });

  it("chooses the operations it faults by grain and state name", async () => {
    const inner = new MemoryGrainStorage();
    const storage = new FaultInjectingGrainStorage(inner);
    storage.fail("write", { when: ({ grainId }) => grainId.type === "Ledger" });

    await expect(storage.write("state", OTHER, holder(1))).rejects.toBeInstanceOf(
      InjectedStorageFault,
    );
    await storage.write("state", ACCOUNT, holder(2));
    expect(await stored(inner, ACCOUNT, 0)).toBe(2);
  });

  it("raises the error it is given", async () => {
    const storage = new FaultInjectingGrainStorage(new MemoryGrainStorage());
    const boom = new Error("disk full");
    storage.fail("read", { error: () => boom });
    await expect(storage.read("state", ACCOUNT, holder({}))).rejects.toBe(boom);
  });

  it("delays an operation until a gate opens, then runs it", async () => {
    const inner = new MemoryGrainStorage();
    const storage = new FaultInjectingGrainStorage(inner);
    let open!: () => void;
    storage.delay("write", { until: new Promise<void>((resolve) => (open = resolve)) });

    const write = storage.write("state", ACCOUNT, holder("late"));
    expect(await settled(write)).toBe(false);
    expect(await stored(inner, ACCOUNT, "absent")).toBe("absent");
    open();
    await write;
    expect(await stored(inner, ACCOUNT, "absent")).toBe("late");
  });

  it("delays an operation by a duration on the clock it is given", async () => {
    const time = new FakeTimeProvider();
    const storage = new FaultInjectingGrainStorage(new MemoryGrainStorage(), { time });
    storage.delay("read", { ms: 50 });
    const read = storage.read("state", ACCOUNT, holder({}));
    expect(await settled(read)).toBe(false);
    time.advance(50);
    expect(await settled(read)).toBe(true);
  });

  it("loses a write: it reports success and stores nothing", async () => {
    const inner = new MemoryGrainStorage();
    const storage = new FaultInjectingGrainStorage(inner);
    storage.lose("write", { count: 1 });
    await storage.write("state", ACCOUNT, holder("lost"));
    expect(await stored(inner, ACCOUNT, "absent")).toBe("absent");
  });

  it("halts: no operation settles any more, as in a process that has gone", async () => {
    const storage = new FaultInjectingGrainStorage(new MemoryGrainStorage());
    storage.halt();
    expect(await settled(storage.read("state", ACCOUNT, holder({})))).toBe(false);
    expect(await settled(storage.write("state", ACCOUNT, holder(1)))).toBe(false);
  });

  it("heals: a healed fault no longer applies", async () => {
    const storage = new FaultInjectingGrainStorage(new MemoryGrainStorage());
    const fault = storage.fail("write");
    fault.heal();
    const written = holder(1);
    await storage.write("state", ACCOUNT, written);
    storage.fail("clear");
    storage.healAll();
    await storage.clear("state", ACCOUNT, written);
  });
});

describe("FaultInjectingJournalStorage", () => {
  it("forwards the backing key, so it shares a log with the store it wraps", () => {
    const inner = new MemoryJournalStorage();
    const journal = new FaultInjectingJournalStorage(inner);
    expect(journal.backingKey).toBe(inner.backingKey);
    expect(sharesJournalBacking(journal, inner)).toBe(true);
  });

  it("has no backing key over a store that has none", () => {
    expect("backingKey" in new FaultInjectingJournalStorage(keyless())).toBe(false);
  });

  it("fails appends it is told to and passes reads, replaces and other appends through", async () => {
    const inner = new MemoryJournalStorage();
    const journal = new FaultInjectingJournalStorage(inner);
    journal.fail("append", { count: 1 });

    await expect(journal.append("log", ACCOUNT, ["e1"], undefined)).rejects.toBeInstanceOf(
      InjectedStorageFault,
    );
    expect((await inner.read("log", ACCOUNT)).entries).toEqual([]);
    const version = await journal.append("log", ACCOUNT, ["e1"], undefined);
    await journal.replace("log", ACCOUNT, ["snapshot"], version);
    expect((await journal.read("log", ACCOUNT)).entries).toEqual(["snapshot"]);
    expect(journal.injected).toBe(1);
  });

  it("delays the operation it is told to", async () => {
    const journal = new FaultInjectingJournalStorage(new MemoryJournalStorage());
    let open!: () => void;
    journal.delay("read", { until: new Promise<void>((resolve) => (open = resolve)) });
    const read = journal.read("log", ACCOUNT);
    expect(await settled(read)).toBe(false);
    open();
    expect((await read).entries).toEqual([]);
  });
});

/** A journal store that declares no backing key. */
function keyless(): JournalStorage {
  const real = new MemoryJournalStorage();
  return {
    read: (logName, grainId) => real.read(logName, grainId),
    append: (logName, grainId, entries, expected) =>
      real.append(logName, grainId, entries, expected),
    replace: (logName, grainId, entries, expected) =>
      real.replace(logName, grainId, entries, expected),
    clear: (logName, grainId) => real.clear(logName, grainId),
  };
}
