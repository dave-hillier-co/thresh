import { describe, expect, it } from "vitest";
import { MemoryStreamCursorStore } from "@thresh/streams/stream-cursor-store";

describe("MemoryStreamCursorStore", () => {
  it("starts at cursor 0 and commits advance it", async () => {
    const store = new MemoryStreamCursorStore();
    expect(await store.getCursor("p", 0)).toBe(0);
    await store.commit("p", 0, 5);
    expect(await store.getCursor("p", 0)).toBe(5);
  });

  // Ownership-handoff regression: during queue-ownership handoff the de-owned
  // agent's fire-and-forget commit can land after the new owner has already
  // committed further ahead. A stale, smaller commit must not rewind the
  // cursor and cause a whole batch to be redelivered.
  it("does not regress the cursor on a stale, smaller commit (issue: ownership handoff)", async () => {
    const store = new MemoryStreamCursorStore();
    await store.commit("p", 0, 10);
    await store.commit("p", 0, 4); // stale commit from the de-owned agent
    expect(await store.getCursor("p", 0)).toBe(10);
  });

  it("still advances on a later, larger commit after a stale one was ignored", async () => {
    const store = new MemoryStreamCursorStore();
    await store.commit("p", 0, 10);
    await store.commit("p", 0, 4);
    await store.commit("p", 0, 15);
    expect(await store.getCursor("p", 0)).toBe(15);
  });

  // seek() is the deliberate escape hatch for RecoverableStreamDeliveryError's
  // checkpoint rewind — unlike commit(), it must go backwards on request.
  it("seek unconditionally rewinds the cursor", async () => {
    const store = new MemoryStreamCursorStore();
    await store.commit("p", 0, 10);
    await store.seek("p", 0, 3);
    expect(await store.getCursor("p", 0)).toBe(3);
  });
});
