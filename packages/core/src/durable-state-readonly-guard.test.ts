import { describe, expect, it } from "vitest";
import { ReadOnlyStateViolationError } from "./errors";
import {
  guardDurableDictionaryForReadOnly,
  guardDurableFieldForReadOnly,
  guardDurableListForReadOnly,
  guardDurableQueueForReadOnly,
  guardDurableSetForReadOnly,
  guardDurableValueForReadOnly,
} from "./durable-state-readonly-guard";
import type {
  DurableDictionary,
  DurableList,
  DurableQueue,
  DurableSet,
  DurableValue,
} from "./durable-state";

function fakeValue<T>(initial: T | undefined): DurableValue<T> & { sets: number; clears: number } {
  let current = initial;
  return {
    get value() {
      return current;
    },
    get() {
      return current;
    },
    sets: 0,
    clears: 0,
    async set(v: T) {
      this.sets++;
      current = v;
    },
    async clear() {
      this.clears++;
      current = undefined;
    },
  };
}

function fakeDictionary<K, V>(): DurableDictionary<K, V> & {
  sets: number;
  deletes: number;
  clears: number;
} {
  const map = new Map<K, V>();
  return {
    get size() {
      return map.size;
    },
    has: (k) => map.has(k),
    get: (k) => map.get(k),
    entries: () => map.entries(),
    keys: () => map.keys(),
    values: () => map.values(),
    sets: 0,
    deletes: 0,
    clears: 0,
    async set(k: K, v: V) {
      this.sets++;
      map.set(k, v);
    },
    async delete(k: K) {
      this.deletes++;
      return map.delete(k);
    },
    async clear() {
      this.clears++;
      map.clear();
    },
  };
}

function fakeList<T>(initial: T[]): DurableList<T> & { adds: number } {
  const items = [...initial];
  return {
    get length() {
      return items.length;
    },
    get: (i) => items[i],
    toArray: () => [...items],
    contains: (v) => items.includes(v),
    [Symbol.iterator]: () => items[Symbol.iterator](),
    adds: 0,
    async add(v: T) {
      this.adds++;
      items.push(v);
    },
    async set(i: number, v: T) {
      items[i] = v;
    },
    async insert(i: number, v: T) {
      items.splice(i, 0, v);
    },
    async removeAt(i: number) {
      items.splice(i, 1);
    },
    async remove(v: T) {
      const i = items.indexOf(v);
      if (i === -1) return false;
      items.splice(i, 1);
      return true;
    },
    async clear() {
      items.length = 0;
    },
  };
}

function fakeQueue<T>(initial: T[]): DurableQueue<T> & { enqueues: number } {
  const items = [...initial];
  return {
    get size() {
      return items.length;
    },
    peek: () => items[0],
    peekOrThrow: () => items[0] as T,
    toArray: () => [...items],
    [Symbol.iterator]: () => items[Symbol.iterator](),
    enqueues: 0,
    async enqueue(v: T) {
      this.enqueues++;
      items.push(v);
    },
    async dequeue() {
      return items.shift();
    },
    async dequeueOrThrow() {
      return items.shift() as T;
    },
    async clear() {
      items.length = 0;
    },
  };
}

function fakeSet<T>(initial: T[]): DurableSet<T> & { adds: number } {
  const values = new Set<T>(initial);
  return {
    get size() {
      return values.size;
    },
    has: (v) => values.has(v),
    values: () => values.values(),
    [Symbol.iterator]: () => values.values(),
    adds: 0,
    async add(v: T) {
      this.adds++;
      values.add(v);
      return true;
    },
    async delete(v: T) {
      return values.delete(v);
    },
    async clear() {
      values.clear();
    },
  };
}

describe("guardDurableValueForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeValue({ cents: 100 });
    const guarded = guardDurableValueForReadOnly(inner, "balance");
    expect(guarded.value).toEqual({ cents: 100 });
    expect(guarded.get()).toEqual({ cents: 100 });
  });

  it("rejects mutation of a nested object reached through .value", () => {
    const inner = fakeValue({ cents: 100 });
    const guarded = guardDurableValueForReadOnly(inner, "balance");
    expect(() => {
      (guarded.value as { cents: number }).cents = 5;
    }).toThrow(ReadOnlyStateViolationError);
  });

  it("rejects set() and clear() without touching the underlying facet", async () => {
    const inner = fakeValue({ cents: 100 });
    const guarded = guardDurableValueForReadOnly(inner, "balance");
    await expect(guarded.set({ cents: 1 })).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.clear()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.sets).toBe(0);
    expect(inner.clears).toBe(0);
  });
});

describe("guardDurableDictionaryForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeDictionary<string, number>();
    void inner.set("a", 1);
    const guarded = guardDurableDictionaryForReadOnly(inner, "map");
    expect(guarded.size).toBe(1);
    expect(guarded.has("a")).toBe(true);
    expect(guarded.get("a")).toBe(1);
    expect([...guarded.keys()]).toEqual(["a"]);
  });

  it("rejects set(), delete() and clear() without touching the underlying facet", async () => {
    const inner = fakeDictionary<string, number>();
    const guarded = guardDurableDictionaryForReadOnly(inner, "map");
    await expect(guarded.set("a", 1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.delete("a")).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.clear()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.sets).toBe(0);
    expect(inner.deletes).toBe(0);
    expect(inner.clears).toBe(0);
  });
});

describe("guardDurableListForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeList([1, 2, 3]);
    const guarded = guardDurableListForReadOnly(inner, "items");
    expect(guarded.length).toBe(3);
    expect(guarded.toArray()).toEqual([1, 2, 3]);
    expect(guarded.contains(2)).toBe(true);
  });

  it("rejects every mutator without touching the underlying facet", async () => {
    const inner = fakeList<number>([1]);
    const guarded = guardDurableListForReadOnly(inner, "items");
    await expect(guarded.add(2)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.set(0, 9)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.insert(0, 9)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.removeAt(0)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.remove(1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.clear()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.adds).toBe(0);
    expect(inner.toArray()).toEqual([1]);
  });
});

describe("guardDurableQueueForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeQueue([1, 2]);
    const guarded = guardDurableQueueForReadOnly(inner, "jobs");
    expect(guarded.size).toBe(2);
    expect(guarded.peek()).toBe(1);
    expect(guarded.toArray()).toEqual([1, 2]);
  });

  it("rejects enqueue/dequeue/clear without touching the underlying facet", async () => {
    const inner = fakeQueue<number>([1]);
    const guarded = guardDurableQueueForReadOnly(inner, "jobs");
    await expect(guarded.enqueue(2)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.dequeue()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.dequeueOrThrow()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.clear()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.enqueues).toBe(0);
    expect(inner.toArray()).toEqual([1]);
  });
});

describe("guardDurableSetForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeSet([1, 2]);
    const guarded = guardDurableSetForReadOnly(inner, "tags");
    expect(guarded.size).toBe(2);
    expect(guarded.has(1)).toBe(true);
    expect([...guarded.values()]).toEqual([1, 2]);
  });

  it("rejects add/delete/clear without touching the underlying facet", async () => {
    const inner = fakeSet<number>([1]);
    const guarded = guardDurableSetForReadOnly(inner, "tags");
    await expect(guarded.add(2)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.delete(1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.clear()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.adds).toBe(0);
    expect([...inner.values()]).toEqual([1]);
  });
});

describe("guardDurableFieldForReadOnly", () => {
  it("dispatches to the matching guard for each DurableKind", async () => {
    expect(guardDurableFieldForReadOnly("value", fakeValue(1), "v")).toHaveProperty("get");
    const dict = guardDurableFieldForReadOnly(
      "dictionary",
      fakeDictionary(),
      "d",
    ) as DurableDictionary<string, number>;
    await expect(dict.set("a", 1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    const list = guardDurableFieldForReadOnly("list", fakeList([]), "l") as DurableList<number>;
    await expect(list.add(1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    const queue = guardDurableFieldForReadOnly("queue", fakeQueue([]), "q") as DurableQueue<number>;
    await expect(queue.enqueue(1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    const set = guardDurableFieldForReadOnly("set", fakeSet([]), "s") as DurableSet<number>;
    await expect(set.add(1)).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
  });
});
