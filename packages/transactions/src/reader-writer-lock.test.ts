import { describe, expect, it } from "vitest";
import { ReaderWriterLock } from "@tsva/transactions/reader-writer-lock";
import { TransactionAbortedError } from "@tsva/core/errors";

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
});
