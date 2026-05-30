import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import { DurableQueueImpl } from "@tsva/journaling/durable-queue-impl";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@tsva/journaling/state-machine-manager-impl";

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
    await queue.enqueue("a");
    await queue.enqueue("b");
    await queue.enqueue("c");
    expect(queue.size).toBe(3);
    expect(queue.peek()).toBe("a");
    expect(queue.toArray()).toEqual(["a", "b", "c"]);
    expect([...queue]).toEqual(["a", "b", "c"]);

    expect(await queue.dequeue()).toBe("a");
    expect(await queue.dequeue()).toBe("b");
    expect(queue.peek()).toBe("c");
    expect(queue.size).toBe(1);

    await queue.clear();
    expect(queue.size).toBe(0);
  });

  it("dequeue on an empty queue resolves to undefined without journaling", async () => {
    const storage = new MemoryJournalStorage();
    const { queue } = makeQueue<number>(storage);
    expect(await queue.dequeue()).toBeUndefined();
    // A no-op dequeue must not have written a log entry.
    const replayed = makeQueue<number>(storage);
    await replayed.manager.replay();
    expect(replayed.queue.size).toBe(0);
  });

  it("rebuilds from the log on a fresh activation", async () => {
    const storage = new MemoryJournalStorage();
    const a = makeQueue<number>(storage);
    await a.queue.enqueue(1);
    await a.queue.enqueue(2);
    await a.queue.enqueue(3);
    await a.queue.dequeue(); // [2,3]
    await a.queue.enqueue(4); // [2,3,4]

    const b = makeQueue<number>(storage);
    await b.manager.replay();
    expect(b.queue.toArray()).toEqual([2, 3, 4]);
  });
});
