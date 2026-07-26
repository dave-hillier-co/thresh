import { describe, expect, it } from "vitest";
import {
  DurableStreamFailureHandler,
  MemoryStreamFailureStore,
} from "@thresh/streams/stream-failure-store";

describe("MemoryStreamFailureStore", () => {
  it("records a failure and lists it back", async () => {
    const store = new MemoryStreamFailureStore();
    await store.recordFailure({
      streamKey: "room/bad",
      event: "poison",
      token: 1,
      error: "boom",
      attempts: 3,
      failedAt: 100,
    });
    expect(await store.listFailures()).toEqual([
      {
        streamKey: "room/bad",
        event: "poison",
        token: 1,
        error: "boom",
        attempts: 3,
        failedAt: 100,
      },
    ]);
  });

  it("narrows listFailures to one stream", async () => {
    const store = new MemoryStreamFailureStore();
    await store.recordFailure({
      streamKey: "room/a",
      event: 1,
      token: 1,
      error: "e1",
      attempts: 3,
      failedAt: 1,
    });
    await store.recordFailure({
      streamKey: "room/b",
      event: 2,
      token: 2,
      error: "e2",
      attempts: 3,
      failedAt: 2,
    });
    expect((await store.listFailures("room/b")).map((f) => f.streamKey)).toEqual(["room/b"]);
  });
});

describe("DurableStreamFailureHandler", () => {
  it("persists a delivery failure to the store, stringifying the error", async () => {
    const store = new MemoryStreamFailureStore();
    const handler = new DurableStreamFailureHandler(store, () => 42);

    await handler.onDeliveryFailure("room/bad", "poison", 7, new Error("always fails"), 3);

    expect(await store.listFailures()).toEqual([
      {
        streamKey: "room/bad",
        event: "poison",
        token: 7,
        error: "always fails",
        attempts: 3,
        failedAt: 42,
      },
    ]);
  });

  it("stringifies a non-Error failure reason", async () => {
    const store = new MemoryStreamFailureStore();
    const handler = new DurableStreamFailureHandler(store, () => 1);

    await handler.onDeliveryFailure("s", "e", 1, "raw string failure", 1);

    expect((await store.listFailures())[0]!.error).toBe("raw string failure");
  });
});
