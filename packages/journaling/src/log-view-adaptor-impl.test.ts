import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";
import { LogViewAdaptorImpl } from "@thresh/journaling/log-view-adaptor-impl";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

/**
 * A journal store whose appends can be made to fail: `failAppends` rejects that many appends
 * before they reach storage; `landThenFail` lets the next append through and THEN rejects it,
 * the ambiguous failure (written, but the caller never heard).
 */
class FlakyJournal implements JournalStorage {
  failAppends = 0;
  landThenFail = false;
  /** Every append that reached storage, by its entry count. */
  readonly appended: number[] = [];

  constructor(private readonly inner: JournalStorage = new MemoryJournalStorage()) {}

  read(logName: string, grainId: GrainId): Promise<JournalSegment> {
    return this.inner.read(logName, grainId);
  }

  async append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    if (this.failAppends > 0) {
      this.failAppends -= 1;
      throw new Error("injected append failure");
    }
    const version = await this.inner.append(logName, grainId, entries, expectedVersion);
    this.appended.push(entries.length);
    if (this.landThenFail) {
      this.landThenFail = false;
      throw new Error("injected failure after the append landed");
    }
    return version;
  }

  replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    return this.inner.replace(logName, grainId, entries, expectedVersion);
  }

  clear(logName: string, grainId: GrainId): Promise<void> {
    return this.inner.clear(logName, grainId);
  }
}

const id = new GrainId("Ledger", "l1");

async function adaptorOver(journal: JournalStorage): Promise<LogViewAdaptorImpl<number, number>> {
  const manager = new StateMachineManagerImpl("journal", id, journal);
  const adaptor = new LogViewAdaptorImpl<number, number>(
    () => 0,
    (state, event) => state + event,
    manager,
  );
  manager.register(adaptor);
  await manager.replay();
  return adaptor;
}

describe("LogViewAdaptorImpl.tryAppend on a failed write", () => {
  it("never lets a later append apply an event its caller was told failed", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);
    expect(await adaptor.tryAppend(1)).toBe(true);

    journal.failAppends = Number.MAX_SAFE_INTEGER;
    expect(await adaptor.tryAppend(10)).toBe(false);
    expect(adaptor.pendingCount).toBe(0);
    expect(adaptor.tentativeView).toBe(1);

    journal.failAppends = 0;
    expect(await adaptor.tryAppend(100)).toBe(true);
    expect(adaptor.confirmedView).toBe(101);

    // And storage agrees: a fresh reader sees exactly the two successful events.
    expect((await adaptorOver(journal)).confirmedView).toBe(101);
  });

  it("retries a transient failure and appends the event exactly once", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);

    journal.failAppends = 1;
    expect(await adaptor.tryAppend(5)).toBe(true);
    expect(adaptor.confirmedView).toBe(5);
    expect(adaptor.confirmedVersion).toBe(1);
    expect((await adaptorOver(journal)).confirmedView).toBe(5);
  });

  it("reports success when re-reading shows the failed write actually landed", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);

    journal.landThenFail = true;
    expect(await adaptor.tryAppend(7)).toBe(true);
    expect(adaptor.confirmedView).toBe(7);
    expect(adaptor.pendingCount).toBe(0);

    // The activation is back in step with storage: the next append does not conflict.
    expect(await adaptor.tryAppend(1)).toBe(true);
    expect((await adaptorOver(journal)).confirmedView).toBe(8);
  });
});

describe("LogViewAdaptorImpl batched appends", () => {
  it("writes every pending event in one storage append", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);

    adaptor.submitRange([1, 2, 3]);
    await adaptor.confirmSubmittedEntries();

    expect(journal.appended).toEqual([3]);
    expect(adaptor.confirmedView).toBe(6);
    expect(adaptor.confirmedVersion).toBe(3);
    expect((await adaptorOver(journal)).confirmedView).toBe(6);
  });

  it("appends a conditional range atomically in one write", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);

    expect(await adaptor.tryAppendRange([1, 2])).toBe(true);

    expect(journal.appended).toEqual([2]);
    expect(adaptor.confirmedView).toBe(3);
    expect(adaptor.pendingCount).toBe(0);
  });

  it("withdraws every event of a range whose write keeps failing", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);

    journal.failAppends = Number.MAX_SAFE_INTEGER;
    expect(await adaptor.tryAppendRange([1, 2])).toBe(false);
    expect(adaptor.pendingCount).toBe(0);
    expect(adaptor.tentativeView).toBe(0);

    journal.failAppends = 0;
    expect(await adaptor.tryAppendRange([10])).toBe(true);
    expect((await adaptorOver(journal)).confirmedView).toBe(10);
  });

  it("reports a range as appended when re-reading shows its write landed", async () => {
    const journal = new FlakyJournal();
    const adaptor = await adaptorOver(journal);

    journal.landThenFail = true;
    expect(await adaptor.tryAppendRange([4, 5])).toBe(true);
    expect(adaptor.confirmedView).toBe(9);
    expect(adaptor.pendingCount).toBe(0);
    expect((await adaptorOver(journal)).confirmedView).toBe(9);
  });
});
