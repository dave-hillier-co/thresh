import { describe, expect, it } from "vitest";
import { ReadOnlyStateViolationError } from "./errors";
import { guardPersistentStateForReadOnly } from "./persistent-state-readonly-guard";
import type { PersistentState } from "./persistent-state";

interface Balance {
  cents: number;
  history: number[];
}

function fakeState(
  initial: Balance,
): PersistentState<Balance> & { writes: number; clears: number } {
  let value = initial;
  let etag: string | undefined;
  let exists = false;
  return {
    get value() {
      return value;
    },
    set value(next: Balance) {
      value = next;
    },
    get etag() {
      return etag;
    },
    get exists() {
      return exists;
    },
    writes: 0,
    clears: 0,
    async read() {
      // no-op: nothing to reload in this fake
    },
    async write() {
      this.writes++;
      etag = "v1";
      exists = true;
    },
    async clear() {
      this.clears++;
      value = initial;
      exists = false;
    },
  };
}

describe("guardPersistentStateForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeState({ cents: 100, history: [1, 2] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    expect(guarded.value.cents).toBe(100);
    expect(guarded.value.history).toEqual([1, 2]);
    expect(guarded.etag).toBe(inner.etag);
    expect(guarded.exists).toBe(inner.exists);
  });

  it("rejects a top-level property write on .value", () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    expect(() => {
      guarded.value.cents = 5;
    }).toThrow(ReadOnlyStateViolationError);
    expect(inner.value.cents).toBe(100);
  });

  it("rejects mutation of a nested array reached through .value", () => {
    const inner = fakeState({ cents: 100, history: [1, 2] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    expect(() => guarded.value.history.push(3)).toThrow(ReadOnlyStateViolationError);
    expect(inner.value.history).toEqual([1, 2]);
  });

  it("rejects replacing .value wholesale", () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    expect(() => {
      guarded.value = { cents: 0, history: [] };
    }).toThrow(ReadOnlyStateViolationError);
  });

  it("rejects write() and clear() without touching the underlying facet", async () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    await expect(guarded.write()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    await expect(guarded.clear()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.writes).toBe(0);
    expect(inner.clears).toBe(0);
  });

  it("allows read() through", async () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    await expect(guarded.read()).resolves.toBeUndefined();
  });

  it("throws a message naming the state", () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardPersistentStateForReadOnly(inner, "balance");
    try {
      guarded.value.cents = 1;
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ReadOnlyStateViolationError);
      expect((err as Error).message).toContain("balance");
    }
  });
});
