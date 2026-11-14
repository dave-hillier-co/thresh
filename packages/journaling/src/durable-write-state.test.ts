import { describe, expect, it } from "vitest";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";
import { DurableDictionaryImpl } from "@thresh/journaling/durable-dictionary-impl";
import { DurableListImpl } from "@thresh/journaling/durable-list-impl";
import { DurableQueueImpl } from "@thresh/journaling/durable-queue-impl";
import { DurableSetImpl } from "@thresh/journaling/durable-set-impl";
import { DurableValueImpl } from "@thresh/journaling/durable-value-impl";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

const id = new GrainId("Agg", "g1");

/** Counts appends and can fail the next one; everything else delegates to a memory store. */
class RecordingJournalStorage implements JournalStorage {
  readonly inner = new MemoryJournalStorage();
  appends: (readonly JournalEntry[])[] = [];
  failNextAppend: Error | undefined;

  read(logName: string, grainId: GrainId, _signal?: AbortSignal): Promise<JournalSegment> {
    return this.inner.read(logName, grainId);
  }

  append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    _signal?: AbortSignal,
  ): Promise<number> {
    if (this.failNextAppend !== undefined) {
      const error = this.failNextAppend;
      this.failNextAppend = undefined;
      return Promise.reject(error);
    }
    this.appends.push(entries);
    return this.inner.append(logName, grainId, entries, expectedVersion);
  }

  replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    _signal?: AbortSignal,
  ): Promise<number> {
    return this.inner.replace(logName, grainId, entries, expectedVersion);
  }

  clear(logName: string, grainId: GrainId, _signal?: AbortSignal): Promise<void> {
    return this.inner.clear(logName, grainId);
  }
}

function activate(storage: JournalStorage, snapshotThreshold?: number) {
  const manager = new StateMachineManagerImpl(
    "journal",
    id,
    storage,
    snapshotThreshold === undefined ? {} : { snapshotThreshold },
  );
  const queue = new DurableQueueImpl<string>("queue", manager);
  const set = new DurableSetImpl<string>("set", manager);
  const value = new DurableValueImpl<number>("value", manager);
  const dict = new DurableDictionaryImpl<string, number>("dict", manager);
  const list = new DurableListImpl<string>("list", manager);
  for (const m of [queue, set, value, dict, list]) manager.register(m);
  return { manager, queue, set, value, dict, list };
}

describe("buffered durable mutations", () => {
  it("returns synchronously and lets a read see the mutation before any write", () => {
    const { queue, set, value, dict, list } = activate(new RecordingJournalStorage());
    // No await anywhere: every mutator is an in-memory operation.
    queue.enqueue("a");
    expect(queue.peek()).toBe("a");
    expect(set.add("x")).toBe(true);
    expect(set.add("x")).toBe(false);
    expect(set.has("x")).toBe(true);
    value.set(5);
    expect(value.value).toBe(5);
    dict.set("k", 1);
    expect(dict.get("k")).toBe(1);
    expect(dict.delete("k")).toBe(true);
    list.add("l");
    expect(list.remove("l")).toBe(true);
    expect(queue.dequeue()).toBe("a");
    expect(queue.size).toBe(0);
  });

  it("costs one storage append for ten mutations followed by one write", async () => {
    const storage = new RecordingJournalStorage();
    const { manager, queue, set, value, dict, list } = activate(storage);
    await manager.replay();
    queue.enqueue("a");
    queue.enqueue("b");
    queue.dequeue();
    set.add("x");
    set.add("y");
    value.set(1);
    value.set(2);
    dict.set("k", 1);
    dict.set("j", 2);
    list.add("l");
    expect(storage.appends).toHaveLength(0);
    await manager.writeState();
    expect(storage.appends).toHaveLength(1);
    expect(storage.appends[0]).toHaveLength(10);
  });

  it("writes nothing when nothing is buffered", async () => {
    const storage = new RecordingJournalStorage();
    const { manager } = activate(storage);
    await manager.replay();
    await manager.writeState();
    expect(storage.appends).toHaveLength(0);
  });

  it("makes a dequeue and a set add durable together in one append, and neither on a crash before the write", async () => {
    const storage = new RecordingJournalStorage();
    const first = activate(storage);
    await first.manager.replay();
    first.queue.enqueue("job");
    await first.manager.writeState();
    storage.appends = [];

    // A crash before the write: a fresh activation sees the pre-method state.
    first.queue.dequeue();
    first.set.add("job");
    const crashed = activate(storage);
    await crashed.manager.replay();
    expect([...crashed.queue]).toEqual(["job"]);
    expect(crashed.set.has("job")).toBe(false);

    // The same method followed by a write: one append carries both.
    await first.manager.writeState();
    expect(storage.appends).toHaveLength(1);
    const after = activate(storage);
    await after.manager.replay();
    expect(after.queue.size).toBe(0);
    expect(after.set.has("job")).toBe(true);
  });

  it("replays buffered entries in the order they were made across structures", async () => {
    const storage = new RecordingJournalStorage();
    const a = activate(storage);
    await a.manager.replay();
    a.list.add("one");
    a.queue.enqueue("q");
    a.list.add("two");
    a.value.set(9);
    await a.manager.writeState();
    const b = activate(storage);
    await b.manager.replay();
    expect(b.list.toArray()).toEqual(["one", "two"]);
    expect([...b.queue]).toEqual(["q"]);
    expect(b.value.value).toBe(9);
  });

  // Mirrors Orleans.Journaling.Tests/StateManagerTests.cs
  // StateManager_WriteStateAsync_PreservesPendingStateAfterTransientStorageWriteFailure, and
  // Orleans.Journaling/JournaledStateManager.cs (WorkLoop, AppendJournalWorkItem): a flush
  // that throws leaves the committed buffer unconsumed, so the entries stay pending and the next
  // WriteStateAsync writes them; nothing in memory is rolled back and the activation continues.
  it("keeps memory and the buffered entries after a failed write, and the next write retries them", async () => {
    const storage = new RecordingJournalStorage();
    const { manager, queue, set } = activate(storage);
    await manager.replay();
    queue.enqueue("a");
    set.add("x");
    storage.failNextAppend = new Error("storage unavailable");
    await expect(manager.writeState()).rejects.toThrow("storage unavailable");
    expect(queue.peek()).toBe("a");
    expect(set.has("x")).toBe(true);

    queue.enqueue("b");
    await manager.writeState();
    expect(storage.appends).toHaveLength(1);
    expect(storage.appends[0]).toHaveLength(3);
    const after = activate(storage);
    await after.manager.replay();
    expect([...after.queue]).toEqual(["a", "b"]);
    expect(after.set.has("x")).toBe(true);
  });

  // Mirrors Orleans.Journaling.Tests/StateManagerTests.cs
  // StateManager_WriteStateAsync_RecoversAfterInconsistentStateException, and
  // JournaledStateManager.cs's IsRecoverySignal(InconsistentStateException) path: the failing write
  // surfaces the exception; the NEXT write first recovers (resets every structure and re-reads the
  // log, discarding what was buffered) and only then proceeds.
  it("recovers from storage on the write after a lost-writer conflict, discarding buffered entries", async () => {
    const storage = new RecordingJournalStorage();
    const mine = activate(storage);
    await mine.manager.replay();
    mine.queue.enqueue("mine");
    await mine.manager.writeState();

    // A foreign incarnation writes first.
    const foreign = activate(storage);
    await foreign.manager.replay();
    foreign.queue.enqueue("theirs");
    await foreign.manager.writeState();

    mine.queue.enqueue("lost");
    await expect(mine.manager.writeState()).rejects.toBeInstanceOf(InconsistentStateError);
    // No eager rollback: memory still holds the lost mutation.
    expect([...mine.queue]).toEqual(["mine", "lost"]);

    storage.appends = [];
    await mine.manager.writeState();
    expect([...mine.queue]).toEqual(["mine", "theirs"]);
    expect(storage.appends).toHaveLength(0);
  });

  // Mirrors Orleans.Journaling.Tests/StateManagerTests.cs StateManager_DirectWriteFailure_AbortsEntryBeforeMutation:
  // an entry that cannot be encoded fails the mutator and leaves memory and the buffer as they were.
  it("leaves memory untouched when a mutation cannot be encoded", async () => {
    const storage = new RecordingJournalStorage();
    const { manager, dict, list } = activate(storage);
    await manager.replay();
    dict.set("ok", 1);
    expect(() => list.add((() => 1) as unknown as string)).toThrow();
    expect(list.length).toBe(0);
    await manager.writeState();
    expect(storage.appends).toHaveLength(1);
    expect(storage.appends[0]).toHaveLength(1);
  });

  it("does not re-read the log after a transient write failure", async () => {
    const storage = new RecordingJournalStorage();
    let reads = 0;
    const read = storage.read.bind(storage);
    storage.read = (logName, grainId, signal) => {
      reads += 1;
      return read(logName, grainId, signal);
    };
    const { manager, queue } = activate(storage);
    await manager.replay();
    queue.enqueue("a");
    storage.failNextAppend = new Error("blip");
    await expect(manager.writeState()).rejects.toThrow("blip");
    await manager.writeState();
    expect(reads).toBe(1);
  });

  it("compacts only after a write, never snapshotting unwritten mutations", async () => {
    const storage = new RecordingJournalStorage();
    const { manager, queue } = activate(storage, 4);
    await manager.replay();
    for (let i = 0; i < 6; i += 1) queue.enqueue(`q${i}`);
    await manager.writeState();
    queue.enqueue("unwritten");
    const after = activate(storage);
    await after.manager.replay();
    expect([...after.queue]).toEqual(["q0", "q1", "q2", "q3", "q4", "q5"]);
  });

  it("serialises overlapping writes so they do not conflict with one another", async () => {
    const storage = new RecordingJournalStorage();
    const { manager, queue } = activate(storage);
    await manager.replay();
    queue.enqueue("a");
    const first = manager.writeState();
    queue.enqueue("b");
    const second = manager.writeState();
    await Promise.all([first, second]);
    const after = activate(storage);
    await after.manager.replay();
    expect([...after.queue]).toEqual(["a", "b"]);
  });
});
