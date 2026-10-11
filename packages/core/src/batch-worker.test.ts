import { describe, expect, it } from "vitest";
import { BatchWorkerFromDelegate } from "./batch-worker";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

describe("BatchWorker", () => {
  it("starts a work cycle on notify, after the notifying turn rather than inside it", async () => {
    let cycles = 0;
    const worker = new BatchWorkerFromDelegate(async () => {
      cycles++;
    });

    worker.notify();
    expect(cycles).toBe(0);
    expect(worker.isIdle()).toBe(false);

    await worker.waitForCurrentWorkToBeServiced();
    expect(cycles).toBe(1);
    expect(worker.isIdle()).toBe(true);
  });

  it("coalesces every notify during a cycle into exactly one more cycle", async () => {
    const gate = deferred();
    let cycles = 0;
    const worker = new BatchWorkerFromDelegate(async () => {
      cycles++;
      if (cycles === 1) await gate.promise;
    });

    worker.notify();
    await settle();
    worker.notify();
    worker.notify();
    worker.notify();
    gate.resolve();
    await worker.waitForCurrentWorkToBeServiced();
    await settle();

    expect(cycles).toBe(2);
    expect(worker.isIdle()).toBe(true);
  });

  it("never runs two cycles at once", async () => {
    let running = 0;
    let maxRunning = 0;
    const worker = new BatchWorkerFromDelegate(async () => {
      running++;
      maxRunning = Math.max(maxRunning, running);
      await settle();
      running--;
    });

    for (let i = 0; i < 5; i++) {
      worker.notify();
      await Promise.resolve();
    }
    while (!worker.isIdle()) await worker.waitForCurrentWorkToBeServiced();

    expect(maxRunning).toBe(1);
  });

  it("waits for the next cycle when work arrived during the current one", async () => {
    const gate = deferred();
    const seen: number[] = [];
    let queued = 1;
    const worker = new BatchWorkerFromDelegate(async () => {
      seen.push(queued);
      if (seen.length === 1) await gate.promise;
    });

    worker.notify();
    await settle();
    queued = 2;
    worker.notify();
    const serviced = worker.waitForCurrentWorkToBeServiced();
    gate.resolve();
    await serviced;

    expect(seen).toEqual([1, 2]);
  });

  it("resolves at once when idle, and notifyAndWait runs a cycle that covers the caller's work", async () => {
    let cycles = 0;
    const worker = new BatchWorkerFromDelegate(async () => {
      cycles++;
    });

    await worker.waitForCurrentWorkToBeServiced();
    expect(cycles).toBe(0);

    await worker.notifyAndWaitForWorkToBeServiced();
    expect(cycles).toBe(1);
  });

  it("keeps working after a cycle rejects, and reports the rejection to a waiter", async () => {
    let cycles = 0;
    const worker = new BatchWorkerFromDelegate(async () => {
      cycles++;
      if (cycles === 1) throw new Error("cycle failed");
    });

    await expect(worker.notifyAndWaitForWorkToBeServiced()).rejects.toThrow("cycle failed");
    await settle();
    expect(worker.isIdle()).toBe(true);

    await worker.notifyAndWaitForWorkToBeServiced();
    expect(cycles).toBe(2);
  });
});
