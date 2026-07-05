import { describe, expect, it } from "vitest";
import {
  claimBudget,
  defaultShouldRetry,
  InMemoryJobQueue,
  shardKeyFor,
  shardStartMs,
} from "@tsva/durable-jobs/job-model";

const HOUR = 3_600_000;

describe("shardKeyFor", () => {
  it("buckets a due time by floor(dueTime / shardDuration)", () => {
    expect(shardKeyFor(0, HOUR)).toBe(0);
    expect(shardKeyFor(HOUR - 1, HOUR)).toBe(0);
    expect(shardKeyFor(HOUR, HOUR)).toBe(1);
    expect(shardKeyFor(HOUR * 2 + 5, HOUR)).toBe(2);
  });

  it("maps the shard key back to the bucket start", () => {
    expect(shardStartMs(2, HOUR)).toBe(2 * HOUR);
  });

  it("rejects a non-positive shard duration", () => {
    expect(() => shardKeyFor(0, 0)).toThrow();
  });
});

describe("defaultShouldRetry", () => {
  it("retries up to 5 attempts with 2^n second backoff, then drops", () => {
    expect(defaultShouldRetry(1, new Error("x"))).toEqual({ seconds: 2 });
    expect(defaultShouldRetry(2, new Error("x"))).toEqual({ seconds: 4 });
    expect(defaultShouldRetry(3, new Error("x"))).toEqual({ seconds: 8 });
    expect(defaultShouldRetry(4, new Error("x"))).toEqual({ seconds: 16 });
    // 5th dequeue is the last attempt: no further retry.
    expect(defaultShouldRetry(5, new Error("x"))).toBeUndefined();
  });
});

describe("claimBudget", () => {
  it("claims at most the budget per step", () => {
    expect(claimBudget(10, 4)).toBe(4);
    expect(claimBudget(2, 4)).toBe(2);
    expect(claimBudget(0, 4)).toBe(0);
  });

  it("never claims with a non-positive budget", () => {
    expect(claimBudget(10, 0)).toBe(0);
  });
});

describe("InMemoryJobQueue", () => {
  it("orders by due time and dequeues only what is due", () => {
    const q = new InMemoryJobQueue();
    q.add({ id: "a", dueMs: 100, dequeueCount: 0, payload: 1 });
    q.add({ id: "b", dueMs: 50, dequeueCount: 0, payload: 2 });
    q.add({ id: "c", dueMs: 300, dequeueCount: 0, payload: 3 });

    expect(q.nextDueMs()).toBe(50);
    const due = q.dequeueDue(100);
    expect(due.map((j) => j.id)).toEqual(["b", "a"]);
    // dequeueDue does not touch dequeueCount — the executor owns it.
    expect(due.every((j) => j.dequeueCount === 0)).toBe(true);
    // c is not yet due and stays.
    expect(q.size).toBe(1);
    expect(q.has("c")).toBe(true);
  });

  it("cancels a pending job", () => {
    const q = new InMemoryJobQueue();
    q.add({ id: "a", dueMs: 100, dequeueCount: 0, payload: 1 });
    expect(q.cancel("a")).toBe(true);
    expect(q.cancel("a")).toBe(false);
    expect(q.size).toBe(0);
  });

  it("retryLater re-enqueues at the caller's dequeue count and a later due time", () => {
    const q = new InMemoryJobQueue();
    q.add({ id: "a", dueMs: 0, dequeueCount: 0, payload: 1 });
    const [job] = q.dequeueDue(0);
    expect(job?.dequeueCount).toBe(0);

    // The executor bumps the count to the attempt just made, then re-enqueues.
    q.retryLater({ ...job!, dequeueCount: 1 }, { seconds: 2 }, 1000);
    const requeued = q.get("a");
    expect(requeued?.dueMs).toBe(3000);
    expect(requeued?.dequeueCount).toBe(1);
  });

  it("retryLater is a no-op for an id the queue does not consider live", () => {
    const q = new InMemoryJobQueue();

    // Never added.
    q.retryLater({ id: "ghost", dueMs: 0, dequeueCount: 0, payload: null }, { seconds: 1 }, 0);
    expect(q.size).toBe(0);
    expect(q.has("ghost")).toBe(false);

    // Added then explicitly cancelled.
    q.add({ id: "a", dueMs: 0, dequeueCount: 0, payload: 1 });
    q.cancel("a");
    q.retryLater({ id: "a", dueMs: 0, dequeueCount: 0, payload: 1 }, { seconds: 1 }, 0);
    expect(q.size).toBe(0);
    expect(q.has("a")).toBe(false);
  });
});
