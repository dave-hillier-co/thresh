import { describe, expect, it } from "vitest";
import { durableQueue, durableSet, grain } from "@thresh/core/decorators";
import { defineGrain, useDurableList, useWriteDurableState } from "@thresh/core/define-grain";
import type { DurableQueue, DurableSet } from "@thresh/core/durable-state";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { castGrainReference } from "@thresh/core/grain-reference";
import type { GrainId } from "@thresh/core/grain-id";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";
import type { GrainKey } from "@thresh/core/key-kinds";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { IGrainManagementExtension } from "@thresh/runtime/grain-management-extension";
import { TestCluster } from "@thresh/testing/test-cluster";

/** A memory journal store that records every append and can fail the next one. */
class RecordingJournalStorage implements JournalStorage {
  readonly inner = new MemoryJournalStorage();
  readonly backingKey = this.inner.backingKey;
  appends: (readonly JournalEntry[])[] = [];
  failNextAppend: Error | undefined;

  read(logName: string, grainId: GrainId): Promise<JournalSegment> {
    return this.inner.read(logName, grainId);
  }

  append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    if (this.failNextAppend !== undefined) {
      const error = this.failNextAppend;
      this.failNextAppend = undefined;
      return Promise.reject(error);
    }
    this.appends.push(entries);
    return this.inner.append(logName, grainId, entries, expectedVersion);
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

interface IJobs extends GrainKey<string> {
  enqueue(job: string): Promise<void>;
  /** Dequeue one job and record it as done, then write once. */
  process(): Promise<string | undefined>;
  /** The same mutations, with no write. */
  processWithoutWrite(): Promise<string | undefined>;
  /** Reads straight after a mutation, in the same turn and before any write. */
  processAndPeek(): Promise<{ queued: number; done: boolean }>;
  write(): Promise<void>;
  queued(): Promise<string[]>;
  done(): Promise<string[]>;
}
const IJobs = defineGrainInterface<IJobs>("IJobsWriteState", {
  options: { queued: { readOnly: true }, done: { readOnly: true } },
});

@grain({ name: "JobsWriteState" })
class JobsGrain extends Grain implements IJobs {
  @durableQueue("queue")
  private queue!: DurableQueue<string>;

  @durableSet("done")
  private doneSet!: DurableSet<string>;

  async enqueue(job: string): Promise<void> {
    this.queue.enqueue(job);
    await this.writeDurableState();
  }

  async process(): Promise<string | undefined> {
    const job = this.queue.dequeue();
    if (job !== undefined) this.doneSet.add(job);
    await this.writeDurableState();
    return job;
  }

  async processWithoutWrite(): Promise<string | undefined> {
    const job = this.queue.dequeue();
    if (job !== undefined) this.doneSet.add(job);
    return job;
  }

  async processAndPeek(): Promise<{ queued: number; done: boolean }> {
    const job = this.queue.dequeue();
    if (job !== undefined) this.doneSet.add(job);
    return { queued: this.queue.size, done: job !== undefined && this.doneSet.has(job) };
  }

  async write(): Promise<void> {
    await this.writeDurableState();
  }

  async queued(): Promise<string[]> {
    return [...this.queue];
  }

  async done(): Promise<string[]> {
    return [...this.doneSet];
  }
}

interface ILog extends GrainKey<string> {
  addTen(): Promise<void>;
  items(): Promise<readonly string[]>;
}
const ILog = defineGrainInterface<ILog>("ILogWriteState", {
  options: { items: { readOnly: true } },
});

const LogGrain = defineGrain<ILog>("LogWriteState", () => {
  const list = useDurableList<string>("items");
  const writeState = useWriteDurableState();
  return {
    addTen: async () => {
      for (let i = 0; i < 10; i += 1) list.add(`item-${i}`);
      await writeState();
    },
    items: async () => list.toArray(),
  };
});

async function startCluster(storage: JournalStorage): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    grains: [
      { ctor: JobsGrain, interfaces: [IJobs] },
      { ctor: LogGrain.grain, interfaces: [ILog] },
    ],
    configureSilo: (builder) => {
      builder.useMemoryJournaling(storage);
    },
  });
}

describe("durable write state at the grain level", () => {
  it("dequeues from one structure and adds to another in exactly one storage append", async () => {
    const storage = new RecordingJournalStorage();
    const cluster = await startCluster(storage);
    try {
      const jobs = cluster.getGrain(IJobs, "a");
      await jobs.enqueue("job-1");
      storage.appends = [];

      expect(await jobs.process()).toBe("job-1");
      expect(storage.appends).toHaveLength(1);

      await cluster.restartSilo(cluster.primary);
      const again = cluster.getGrain(IJobs, "a");
      expect(await again.queued()).toEqual([]);
      expect(await again.done()).toEqual(["job-1"]);
    } finally {
      await cluster.dispose();
    }
  });

  it("leaves neither change durable when the silo crashes before the write", async () => {
    const storage = new RecordingJournalStorage();
    const cluster = await startCluster(storage);
    try {
      const jobs = cluster.getGrain(IJobs, "b");
      await jobs.enqueue("job-1");
      storage.appends = [];

      await jobs.processWithoutWrite();
      expect(storage.appends).toHaveLength(0);

      // restartSilo crashes the silo (no shutdown code runs) and starts a fresh one.
      await cluster.restartSilo(cluster.primary);
      const again = cluster.getGrain(IJobs, "b");
      expect(await again.queued()).toEqual(["job-1"]);
      expect(await again.done()).toEqual([]);
    } finally {
      await cluster.dispose();
    }
  });

  it("returns synchronously and reads its own mutation before any write", async () => {
    const storage = new RecordingJournalStorage();
    const cluster = await startCluster(storage);
    try {
      const jobs = cluster.getGrain(IJobs, "c");
      await jobs.enqueue("job-1");
      await jobs.enqueue("job-2");
      storage.appends = [];

      expect(await jobs.processAndPeek()).toEqual({ queued: 1, done: true });
      expect(storage.appends).toHaveLength(0);
      // A later turn in the same activation also sees it, still unwritten.
      expect(await jobs.queued()).toEqual(["job-2"]);
    } finally {
      await cluster.dispose();
    }
  });

  // Mirrors Orleans.Journaling/JournaledStateManager.cs: StopAsync cancels the work loop and
  // nothing flushes what the structures have buffered, so a deactivation without a write drops
  // those mutations; the next activation replays only what a write made durable.
  it("drops mutations still buffered when the activation deactivates", async () => {
    const storage = new RecordingJournalStorage();
    const cluster = await startCluster(storage);
    try {
      const jobs = cluster.getGrain(IJobs, "d");
      await jobs.enqueue("job-1");
      await jobs.processWithoutWrite();
      expect(await jobs.done()).toEqual(["job-1"]);

      await castGrainReference(jobs, IGrainManagementExtension).deactivateOnIdle();

      expect(await jobs.queued()).toEqual(["job-1"]);
      expect(await jobs.done()).toEqual([]);
    } finally {
      await cluster.dispose();
    }
  });

  // Mirrors Orleans.Journaling.Tests/StateManagerTests.cs
  // StateManager_WriteStateAsync_PreservesPendingStateAfterTransientStorageWriteFailure: the
  // failing write throws to its caller, the activation carries on with its memory intact, and
  // the next write persists what the failed one could not.
  it("surfaces a failed write, keeps the activation and its memory, and retries on the next write", async () => {
    const storage = new RecordingJournalStorage();
    const cluster = await startCluster(storage);
    try {
      const jobs = cluster.getGrain(IJobs, "e");
      await jobs.enqueue("job-1");

      storage.failNextAppend = new Error("storage unavailable");
      await expect(jobs.process()).rejects.toThrow("storage unavailable");
      // Same activation, memory not rolled back.
      expect(await jobs.queued()).toEqual([]);
      expect(await jobs.done()).toEqual(["job-1"]);

      await jobs.write();
      await cluster.restartSilo(cluster.primary);
      const again = cluster.getGrain(IJobs, "e");
      expect(await again.queued()).toEqual([]);
      expect(await again.done()).toEqual(["job-1"]);
    } finally {
      await cluster.dispose();
    }
  });

  it("costs one append for ten mutations followed by one write (hooks)", async () => {
    const storage = new RecordingJournalStorage();
    const cluster = await startCluster(storage);
    try {
      const log = cluster.getGrain(ILog, "f");
      await log.addTen();
      expect(storage.appends).toHaveLength(1);
      expect(storage.appends[0]).toHaveLength(10);
      expect(await log.items()).toHaveLength(10);
    } finally {
      await cluster.dispose();
    }
  });
});
