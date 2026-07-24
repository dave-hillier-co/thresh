import { describe, expect, it } from "vitest";
import { ReadOnlyStateViolationError } from "./errors";
import { guardReducerStateForReadOnly } from "./reducer-state-readonly-guard";
import type { ReducerState } from "./reducer-state";

interface Balance {
  cents: number;
  history: number[];
}

type Deposit = { amount: number };

function fakeState(
  initial: Balance,
): ReducerState<Balance, Deposit> & { raises: number; writes: number } {
  let value = initial;
  return {
    get value() {
      return value;
    },
    etag: undefined,
    exists: false,
    raises: 0,
    writes: 0,
    raise(event: Deposit) {
      this.raises++;
      value = { cents: value.cents + event.amount, history: [...value.history, event.amount] };
      return value;
    },
    async read() {
      // no-op: nothing to reload in this fake
    },
    async write() {
      this.writes++;
    },
  };
}

describe("guardReducerStateForReadOnly", () => {
  it("passes reads through unchanged", () => {
    const inner = fakeState({ cents: 100, history: [1, 2] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    expect(guarded.value.cents).toBe(100);
    expect(guarded.value.history).toEqual([1, 2]);
    expect(guarded.etag).toBe(inner.etag);
    expect(guarded.exists).toBe(inner.exists);
  });

  it("rejects a top-level property write reached through .value", () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    expect(() => {
      (guarded.value as Balance).cents = 5;
    }).toThrow(ReadOnlyStateViolationError);
    expect(inner.value.cents).toBe(100);
  });

  it("rejects mutation of a nested array reached through .value", () => {
    const inner = fakeState({ cents: 100, history: [1, 2] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    expect(() => guarded.value.history.push(3)).toThrow(ReadOnlyStateViolationError);
    expect(inner.value.history).toEqual([1, 2]);
  });

  it("rejects raise() without touching the underlying facet", () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    expect(() => guarded.raise({ amount: 5 })).toThrow(ReadOnlyStateViolationError);
    expect(inner.raises).toBe(0);
    expect(inner.value.cents).toBe(100);
  });

  it("rejects write() without touching the underlying facet", async () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    await expect(guarded.write()).rejects.toBeInstanceOf(ReadOnlyStateViolationError);
    expect(inner.writes).toBe(0);
  });

  it("allows read() through", async () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    await expect(guarded.read()).resolves.toBeUndefined();
  });

  it("throws a message naming the state", () => {
    const inner = fakeState({ cents: 100, history: [] });
    const guarded = guardReducerStateForReadOnly(inner, "balance");
    try {
      guarded.raise({ amount: 1 });
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ReadOnlyStateViolationError);
      expect((err as Error).message).toContain("balance");
    }
  });
});
