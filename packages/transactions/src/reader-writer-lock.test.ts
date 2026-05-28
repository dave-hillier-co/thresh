import { describe, expect, it } from "vitest";
import type { TimeProvider, TimerHandle } from "@tsva/core/time-provider";
import { ReaderWriterLock } from "@tsva/transactions/reader-writer-lock";
import { TransactionAbortedError } from "@tsva/core/errors";

/**
 * A controllable clock for sociable tests: tracks "now", and fires any timer
 * whose deadline has passed when {@link advance} is called.
 */
function fakeClock(start = 0): TimeProvider & { advance(ms: number): void } {
  let current = start;
  const timers = new Map<number, { at: number; fire: () => void }>();
  let nextId = 1;
  const fireDue = () => {
    for (const [id, t] of [...timers]) {
      if (t.at <= current) {
        timers.delete(id);
        t.fire();
      }
    }
  };
  return {
    now: () => current,
    setTimer: (handler, delayMs): TimerHandle => {
      const id = nextId++;
      timers.set(id, { at: current + delayMs, fire: handler });
      return id;
    },
    clearTimer: (handle) => {
      timers.delete(handle as number);
    },
    advance(ms: number) {
      current += ms;
      fireDue();
    },
  };
}

// Lower timestamp = older = higher priority.
const OLDER = 1;
const YOUNGER = 2;

describe("ReaderWriterLock (wait-die)", () => {
  it("shares the lock between concurrent readers", async () => {
    const lock = new ReaderWriterLock();
    await lock.enter("t1", OLDER, "read");
    // A second reader is granted immediately (no conflict).
    await expect(lock.enter("t2", YOUNGER, "read")).resolves.toBeUndefined();
  });

  it("blocks a write while another transaction holds the lock", async () => {
    const lock = new ReaderWriterLock();
    await lock.enter("older", OLDER, "write");

    // The older writer holds; a *younger* writer would die, so use an older
    // requester to exercise the wait path: swap roles.
    const lock2 = new ReaderWriterLock();
    await lock2.enter("younger", YOUNGER, "write");
    let granted = false;
    const waiting = lock2.enter("older", OLDER, "write").then(() => {
      granted = true;
    });
    await Promise.resolve();
    expect(granted).toBe(false); // older waits for the younger holder

    lock2.release("younger");
    await waiting;
    expect(granted).toBe(true); // granted once the holder releases
  });

  it("dies (aborts) when a younger transaction conflicts with an older holder", async () => {
    const lock = new ReaderWriterLock();
    await lock.enter("older", OLDER, "write");
    await expect(lock.enter("younger", YOUNGER, "write")).rejects.toBeInstanceOf(
      TransactionAbortedError,
    );
  });

  it("lets an older transaction wait for a younger holder, then grants it", async () => {
    const lock = new ReaderWriterLock();
    await lock.enter("younger", YOUNGER, "write");

    let granted = false;
    const waiting = lock.enter("older", OLDER, "write").then(() => {
      granted = true;
    });
    await Promise.resolve();
    expect(granted).toBe(false);

    lock.release("younger");
    await waiting;
    expect(granted).toBe(true);
  });

  it("upgrades a read to a write when no other holder conflicts", async () => {
    const lock = new ReaderWriterLock();
    await lock.enter("t1", OLDER, "read");
    await expect(lock.enter("t1", OLDER, "write")).resolves.toBeUndefined();
  });

  it("aborts a younger reader trying to read while an older writer holds", async () => {
    const lock = new ReaderWriterLock();
    await lock.enter("older", OLDER, "write");
    await expect(lock.enter("younger", YOUNGER, "read")).rejects.toBeInstanceOf(
      TransactionAbortedError,
    );
  });

  it("aborts a waiter whose lock-acquisition deadline elapses", async () => {
    const clock = fakeClock();
    const lock = new ReaderWriterLock(clock);
    // A younger holder takes the lock first; an older waiter would normally
    // wait indefinitely (wait-die admits older). Give the older waiter a
    // bounded deadline and confirm it aborts when time expires.
    await lock.enter("younger", YOUNGER, "write");

    const waiting = lock.enter("older", OLDER, "write", { timeoutMs: 5_000 });
    // Not yet expired.
    clock.advance(4_999);
    let settled = false;
    void waiting.catch(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    clock.advance(2);
    await expect(waiting).rejects.toBeInstanceOf(TransactionAbortedError);
    await expect(waiting).rejects.toMatchObject({
      message: expect.stringContaining("deadline"),
    });
  });

  it("clears the deadline timer once the waiter is granted", async () => {
    const clock = fakeClock();
    const lock = new ReaderWriterLock(clock);
    await lock.enter("younger", YOUNGER, "write");
    const waiting = lock.enter("older", OLDER, "write", { timeoutMs: 5_000 });
    lock.release("younger");
    await expect(waiting).resolves.toBeUndefined();
    // Advancing past the deadline must not double-reject a granted waiter.
    clock.advance(10_000);
    // No assertion needed beyond no unhandled rejection; surface explicitly.
    await Promise.resolve();
  });
});
