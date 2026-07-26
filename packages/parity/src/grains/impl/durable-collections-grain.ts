// Test-scaffolding grain implementation for Orleans.Journaling.Tests, standing in
// for upstream's ITestDurableGrain / ITestDurableGrainWithComplexState /
// ITestMultiCollectionGrain / ITestDurableGrainInterface (test/Grains/TestGrains).
import {
  durableDictionary,
  durableList,
  durableQueue,
  durableSet,
  durableState,
  grain,
} from "@thresh/core/decorators";
import type {
  DurableDictionary,
  DurableList,
  DurableQueue,
  DurableSet,
  DurableValue,
} from "@thresh/core/durable-state";
import { Grain } from "@thresh/core/grain";
import { IDurableCollectionsGrain } from "@thresh/parity/grains/interfaces/durable-collections-grain-interfaces";

export { IDurableCollectionsGrain };

@grain({ name: "Orleans.Journaling.Tests.DurableCollectionsGrain" })
export class DurableCollectionsGrain extends Grain implements IDurableCollectionsGrain {
  // A fresh random tag per constructed instance: the same instance never gets a
  // new tag across calls within one activation, but a silo restart constructs a
  // brand-new instance (and replays durable state from the shared journal),
  // giving this the same "new activation" signal as Orleans' ActivationId.
  private readonly tag = crypto.randomUUID();

  @durableState("value")
  private value!: DurableValue<unknown>;

  @durableDictionary("dict")
  private dict!: DurableDictionary<unknown, unknown>;

  @durableList("list")
  private list!: DurableList<unknown>;

  @durableQueue("queue")
  private queue!: DurableQueue<unknown>;

  @durableSet("set")
  private set!: DurableSet<unknown>;

  async setValue(v: unknown): Promise<void> {
    await this.value.set(v);
  }

  async getValue(): Promise<unknown> {
    return this.value.value;
  }

  async clearValue(): Promise<void> {
    await this.value.clear();
  }

  async dictSet(key: unknown, value: unknown): Promise<void> {
    await this.dict.set(key, value);
  }

  async dictGet(key: unknown): Promise<unknown> {
    return this.dict.get(key);
  }

  async dictDelete(key: unknown): Promise<boolean> {
    return this.dict.delete(key);
  }

  async dictHas(key: unknown): Promise<boolean> {
    return this.dict.has(key);
  }

  async dictClear(): Promise<void> {
    await this.dict.clear();
  }

  async dictSize(): Promise<number> {
    return this.dict.size;
  }

  async dictKeys(): Promise<unknown[]> {
    return [...this.dict.keys()];
  }

  async dictValues(): Promise<unknown[]> {
    return [...this.dict.values()];
  }

  async dictEntries(): Promise<[unknown, unknown][]> {
    return [...this.dict.entries()];
  }

  async listAdd(v: unknown): Promise<void> {
    await this.list.add(v);
  }

  async listSet(i: number, v: unknown): Promise<void> {
    await this.list.set(i, v);
  }

  async listRemoveAt(i: number): Promise<void> {
    await this.list.removeAt(i);
  }

  async listGet(i: number): Promise<unknown> {
    return this.list.get(i);
  }

  async listClear(): Promise<void> {
    await this.list.clear();
  }

  async listLength(): Promise<number> {
    return this.list.length;
  }

  async listToArray(): Promise<unknown[]> {
    return [...this.list.toArray()];
  }

  async listContains(v: unknown): Promise<boolean> {
    return this.list.contains(v);
  }

  async listRemove(v: unknown): Promise<boolean> {
    return this.list.remove(v);
  }

  async listInsert(i: number, v: unknown): Promise<void> {
    await this.list.insert(i, v);
  }

  async queueEnqueue(v: unknown): Promise<void> {
    await this.queue.enqueue(v);
  }

  async queueDequeue(): Promise<unknown> {
    return this.queue.dequeue();
  }

  async queuePeek(): Promise<unknown> {
    return this.queue.peek();
  }

  async queuePeekOrThrow(): Promise<unknown> {
    return this.queue.peekOrThrow();
  }

  async queueDequeueOrThrow(): Promise<unknown> {
    return this.queue.dequeueOrThrow();
  }

  async queueClear(): Promise<void> {
    await this.queue.clear();
  }

  async queueSize(): Promise<number> {
    return this.queue.size;
  }

  async queueToArray(): Promise<unknown[]> {
    return [...this.queue.toArray()];
  }

  async setAdd(v: unknown): Promise<boolean> {
    return this.set.add(v);
  }

  async setDelete(v: unknown): Promise<boolean> {
    return this.set.delete(v);
  }

  async setClear(): Promise<void> {
    await this.set.clear();
  }

  async setSize(): Promise<number> {
    return this.set.size;
  }

  async setToArray(): Promise<unknown[]> {
    return [...this.set.values()];
  }

  async activationTag(): Promise<string> {
    return this.tag;
  }
}
