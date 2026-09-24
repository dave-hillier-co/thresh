import { describe, expect, it } from "vitest";
import { GrainCallTimeoutError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { FanOutDelivery } from "@thresh/streams/fan-out-delivery";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import { RecoverableStreamDeliveryError } from "@thresh/streams/stream-recovery";

const A = new GrainId("Consumer", "a");
const B = new GrainId("Consumer", "b");
const C = new GrainId("Consumer", "c");

/** Flush pending microtasks (promise reactions already queued). */
async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

/**
 * Drives the fake clock forward `stepMs` at a time — flushing microtasks
 * between steps so a chain of sequential `setTimer` retries plays out one
 * link at a time, the way `FakeTimeProvider.advance` requires — until
 * `target` settles or `maxSteps` is exhausted.
 */
async function advanceUntilSettled(
  time: FakeTimeProvider,
  stepMs: number,
  target: Promise<unknown>,
  maxSteps = 1000,
): Promise<void> {
  let settled = false;
  target.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  for (let i = 0; i < maxSteps && !settled; i++) {
    time.advance(stepMs);
    await tick();
  }
}

describe("FanOutDelivery", () => {
  it("delivers to every subscriber concurrently: a permanently-failing subscriber never blocks the others", async () => {
    const time = new FakeTimeProvider();
    const delivered: string[] = [];
    const fanOut = new FanOutDelivery(
      async (subscriber) => {
        if (subscriber.equals(B)) throw new Error("boom");
        delivered.push(subscriber.toString());
      },
      { time, maxEventDeliveryTimeMs: 1_000, retryBackoffMs: () => 100 },
    );

    const done = fanOut.deliverToAll([A, B, C], "room/x", "evt", 1);
    // A and C settle immediately; B's retry loop is still running.
    await tick();
    expect(delivered.sort()).toEqual([A.toString(), C.toString()]);

    await advanceUntilSettled(time, 100, done);
    // Neither A nor C was redelivered while B kept retrying and got skipped.
    expect(delivered.sort()).toEqual([A.toString(), C.toString()]);
  });

  it("retries only the failing subscriber, without redelivering to the ones that already succeeded", async () => {
    const time = new FakeTimeProvider();
    const attempts: Record<string, number> = { a: 0, b: 0, c: 0 };
    const fanOut = new FanOutDelivery(
      async (subscriber) => {
        const key = subscriber.key as string;
        attempts[key] = (attempts[key] ?? 0) + 1;
        if (subscriber.equals(B) && attempts[key]! < 2) throw new Error("transient");
      },
      { time, retryBackoffMs: () => 100 },
    );

    const done = fanOut.deliverToAll([A, B, C], "room/x", "evt", 1);
    await advanceUntilSettled(time, 100, done);

    expect(attempts).toEqual({ a: 1, b: 2, c: 1 });
  });

  it("skips only the exhausted subscriber after maxEventDeliveryTimeMs, notifying the failure handler for it alone", async () => {
    const time = new FakeTimeProvider();
    const delivered: string[] = [];
    const failures: Array<{ streamKey: string; token: number }> = [];
    const failureHandler: StreamFailureHandler = {
      async onDeliveryFailure(streamKey, _event, token) {
        failures.push({ streamKey, token });
      },
    };
    const fanOut = new FanOutDelivery(
      async (subscriber) => {
        if (subscriber.equals(B)) throw new Error("always fails");
        delivered.push(subscriber.toString());
      },
      { time, maxEventDeliveryTimeMs: 1_000, retryBackoffMs: () => 100, failureHandler },
    );

    const done = fanOut.deliverToAll([A, B, C], "room/bad", "poison", 7);
    await advanceUntilSettled(time, 100, done);

    expect(delivered.sort()).toEqual([A.toString(), C.toString()]);
    expect(failures).toEqual([{ streamKey: "room/bad", token: 7 }]);
  });

  it("bounds one attempt by deliveryResponseTimeoutMs so a hung deliver is retried rather than blocking forever", async () => {
    const time = new FakeTimeProvider();
    let calls = 0;
    const fanOut = new FanOutDelivery(
      async () => {
        calls++;
        if (calls === 1) return new Promise<void>(() => {}); // hangs forever
      },
      { time, deliveryResponseTimeoutMs: 500, retryBackoffMs: () => 1 },
    );

    const done = fanOut.deliverToAll([A], "s", "e", 1);
    await advanceUntilSettled(time, 100, done);

    expect(calls).toBe(2);
  });

  it("rejects a hung attempt with GrainCallTimeoutError once its response deadline passes", async () => {
    let lastError: unknown;
    const time = new FakeTimeProvider();
    const fanOut = new FanOutDelivery(async () => new Promise<void>(() => {}), {
      time,
      deliveryResponseTimeoutMs: 500,
      maxEventDeliveryTimeMs: 0,
      retryBackoffMs: () => 1,
      failureHandler: {
        onDeliveryFailure: (_s, _e, _t, error) => {
          lastError = error;
        },
      },
    });

    const done = fanOut.deliverToAll([A], "s", "e", 1);
    await advanceUntilSettled(time, 100, done);

    expect(lastError).toBeInstanceOf(GrainCallTimeoutError);
  });

  it("defaults maxEventDeliveryTimeMs to Orleans' 1-minute MaxEventDeliveryTime", async () => {
    const time = new FakeTimeProvider();
    const failures: unknown[] = [];
    const fanOut = new FanOutDelivery(
      async () => {
        throw new Error("always fails");
      },
      {
        time,
        retryBackoffMs: () => 5_000,
        failureHandler: { onDeliveryFailure: (...args) => void failures.push(args) },
      },
    );

    const done = fanOut.deliverToAll([A], "s", "e", 1);
    await tick();
    time.advance(59_000);
    await tick();
    expect(failures).toEqual([]); // not yet skipped: under a minute

    await advanceUntilSettled(time, 5_000, done);
    expect(failures).toHaveLength(1);
  });

  it("defaults deliveryResponseTimeoutMs to Orleans' 30-second ResponseTimeout", async () => {
    const time = new FakeTimeProvider();
    let calls = 0;
    const fanOut = new FanOutDelivery(
      async () => {
        calls++;
        if (calls === 1) return new Promise<void>(() => {});
      },
      { time, retryBackoffMs: () => 1 },
    );

    const done = fanOut.deliverToAll([A], "s", "e", 1);
    await tick();
    time.advance(29_000);
    await tick();
    expect(calls).toBe(1); // not yet timed out

    await advanceUntilSettled(time, 1_000, done);
    expect(calls).toBe(2);
  });

  it("never retries a RecoverableStreamDeliveryError and rethrows it once every subscriber has settled", async () => {
    const time = new FakeTimeProvider();
    const delivered: string[] = [];
    const fanOut = new FanOutDelivery(
      async (subscriber) => {
        if (subscriber.equals(B)) throw new RecoverableStreamDeliveryError("resync", 3);
        delivered.push(subscriber.toString());
      },
      { time },
    );

    await expect(fanOut.deliverToAll([A, B, C], "s", "e", 1)).rejects.toBeInstanceOf(
      RecoverableStreamDeliveryError,
    );
    expect(delivered.sort()).toEqual([A.toString(), C.toString()]);
  });
});
