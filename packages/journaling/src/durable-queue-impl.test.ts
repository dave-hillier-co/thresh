import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { DurableQueueImpl } from "@thresh/journaling/durable-queue-impl";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

const id = new GrainId("Inbox", "i1");

function makeQueue<T>(storage: MemoryJournalStorage) {
  const manager = new StateMachineManagerImpl("journal", id, storage);
  const queue = new DurableQueueImpl<T>("q", manager);
  manager.register(queue);
  return { manager, queue };
}

describe("DurableQueueImpl", () => {
  it("tracks enqueue/dequeue/peek/clear in FIFO order", async () => {
    const { queue } = makeQueue<string>(new MemoryJournalStorage());
    queue.enqueue("a");
    queue.enqueue("b");
    queue.enqueue("c");
    expect(queue.size).toBe(3);
    expect(queue.peek()).toBe("a");
    expect(queue.toArray()).toEqual(["a", "b", "c"]);
    expect([...queue]).toEqual(["a", "b", "c"]);

    expect(queue.dequeue()).toBe("a");
    expect(queue.dequeue()).toBe("b");
    expect(queue.peek()).toBe("c");
    expect(queue.size).toBe(1);

    queue.clear();
    expect(queue.size).toBe(0);
  });

  it("dequeue on an empty queue returns undefined without journaling", async () => {
    const storage = new MemoryJournalStorage();
    const { manager, queue } = makeQueue<number>(storage);
    expect(queue.dequeue()).toBeUndefined();
    await manager.writeState();
    // A no-op dequeue must not have written a log entry.
    const replayed = makeQueue<number>(storage);
    await replayed.manager.replay();
    expect(replayed.queue.size).toBe(0);
  });

  it("peekOrThrow returns the front element without removing it", async () => {
    const { queue } = makeQueue<string>(new MemoryJournalStorage());
    queue.enqueue("a");
    queue.enqueue("b");
    expect(queue.peekOrThrow()).toBe("a");
    expect(queue.size).toBe(2);
  });

  it("peekOrThrow throws on an empty queue", async () => {
    const { queue } = makeQueue<string>(new MemoryJournalStorage());
    expect(() => queue.peekOrThrow()).toThrow(/empty/i);
  });

  it("dequeueOrThrow removes and returns the front element", async () => {
    const { queue } = makeQueue<string>(new MemoryJournalStorage());
    queue.enqueue("a");
    queue.enqueue("b");
    expect(queue.dequeueOrThrow()).toBe("a");
    expect(queue.size).toBe(1);
  });

  it("dequeueOrThrow throws on an empty queue without journaling", async () => {
    const storage = new MemoryJournalStorage();
    const { manager, queue } = makeQueue<string>(storage);
    expect(() => queue.dequeueOrThrow()).toThrow(/empty/i);
    await manager.writeState();

    const replayed = makeQueue<string>(storage);
    await replayed.manager.replay();
    expect(replayed.queue.size).toBe(0);
  });

  it("rebuilds from the log on a fresh activation", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeQueue<number>(storage);
    a.queue.enqueue(1);
    a.queue.enqueue(2);
    a.queue.enqueue(3);
    a.queue.dequeue(); // [2,3]
    a.queue.enqueue(4); // [2,3,4]
    await a.manager.writeState();

    const b = makeQueue<number>(storage);
    await b.manager.replay();
    expect(b.queue.toArray()).toEqual([2, 3, 4]);
  });
});
