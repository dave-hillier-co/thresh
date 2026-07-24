import { describe, expect, it } from "vitest";
import { ReadOnlyStateViolationError } from "./errors";
import { guardTransactionalStateForReadOnly } from "./transactional-state-readonly-guard";
import type { TransactionalState } from "./transactional-state";

interface Balance {
  cents: number;
}

function fakeState(initial: Balance): TransactionalState<Balance> & { updates: number } {
  const value = initial;
  return {
    updates: 0,
    async performRead<R>(read: (state: Balance) => R): Promise<R> {
      return read(value);
    },
    async performUpdate<R>(update: (state: Balance) => R): Promise<R> {
      this.updates++;
      return update(value);
    },
  };
}

describe("guardTransactionalStateForReadOnly", () => {
  it("passes performRead through unchanged", async () => {
    const inner = fakeState({ cents: 100 });
    const guarded = guardTransactionalStateForReadOnly(inner, "balance");
    await expect(guarded.performRead((s) => s.cents)).resolves.toBe(100);
  });

  it("rejects performUpdate without touching the underlying facet or acquiring a lock", async () => {
    const inner = fakeState({ cents: 100 });
    const guarded = guardTransactionalStateForReadOnly(inner, "balance");
    await expect(guarded.performUpdate((s) => (s.cents = 5))).rejects.toBeInstanceOf(
      ReadOnlyStateViolationError,
    );
    expect(inner.updates).toBe(0);
  });

  it("throws a message naming the state", async () => {
    const inner = fakeState({ cents: 100 });
    const guarded = guardTransactionalStateForReadOnly(inner, "balance");
    try {
      await guarded.performUpdate((s) => s.cents);
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ReadOnlyStateViolationError);
      expect((err as Error).message).toContain("balance");
    }
  });
});
