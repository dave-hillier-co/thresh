import { describe, expect, it, vi } from "vitest";
import {
  KafkaPartitionOwner,
  type PartitionOwnershipClient,
} from "@thresh/streams/kafka-partition-owner";

/** Deferred acquire the test drives by hand, to land it exactly mid-flight. */
function deferred<T>(): {
  promise: Promise<T>;
  resolve: (v: T) => void;
  reject: (e: unknown) => void;
} {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("KafkaPartitionOwner", () => {
  it("acquires every newly wanted partition and reports the owned set", async () => {
    const acquired: number[] = [];
    const released: number[] = [];
    const client: PartitionOwnershipClient = {
      acquire: async (i) => void acquired.push(i),
      release: (i) => void released.push(i),
    };
    const snapshots: number[][] = [];
    const owner = new KafkaPartitionOwner(client, (owned) => snapshots.push([...owned].sort()));

    owner.setWanted([0, 1]);
    await Promise.resolve();
    await Promise.resolve();

    expect(acquired.sort()).toEqual([0, 1]);
    expect(released).toEqual([]);
    expect([...owner.currentlyOwned()].sort()).toEqual([0, 1]);
  });

  it("releases a partition no longer wanted, synchronously", () => {
    const released: number[] = [];
    const client: PartitionOwnershipClient = {
      acquire: async () => undefined,
      release: (i) => void released.push(i),
    };
    const owner = new KafkaPartitionOwner(client, () => undefined);

    // Simulate 0 already owned by acquiring it and letting it resolve first
    // isn't needed here — release only ever needs to run for indices already
    // in the owned set, which setWanted maintains internally.
    owner.setWanted([0, 1]);
    return Promise.resolve()
      .then(() => Promise.resolve())
      .then(() => {
        owner.setWanted([1]);
        expect(released).toEqual([0]);
        expect([...owner.currentlyOwned()].sort()).toEqual([1]);
      });
  });

  it("stale acquire: releases a partition immediately if it's no longer wanted by the time acquire resolves (issue #113)", async () => {
    const released: number[] = [];
    const acquireCalls = deferred<void>();
    const client: PartitionOwnershipClient = {
      acquire: (i) => (i === 0 ? acquireCalls.promise : Promise.resolve()),
      release: (i) => void released.push(i),
    };
    const snapshots: number[][] = [];
    const owner = new KafkaPartitionOwner(client, (owned) => snapshots.push([...owned].sort()));

    // Partition 0's acquire is now in flight (not yet resolved).
    owner.setWanted([0]);

    // A membership change arrives before the acquire settles: partition 0 is
    // no longer wanted. It isn't in `owned` yet (still acquiring), so the
    // release loop in `setWanted` has nothing to release.
    owner.setWanted([]);
    expect(released).toEqual([]); // not released yet — nothing to release

    // The in-flight acquire now resolves.
    acquireCalls.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    // It must not be adopted into the owned set (that would mean this silo
    // keeps consuming a partition it no longer owns on the ring) — it must
    // be released instead.
    expect(released).toEqual([0]);
    expect(owner.currentlyOwned().has(0)).toBe(false);
    expect(snapshots.every((s) => !s.includes(0))).toBe(true);
  });

  it("retries a failed acquire with backoff while the partition is still wanted (issue #113)", async () => {
    let attempts = 0;
    const client: PartitionOwnershipClient = {
      acquire: async (_i) => {
        attempts++;
        if (attempts < 3) throw new Error("cursor store blip");
      },
      release: () => undefined,
    };
    const sleeps: number[] = [];
    const fakeSleep = (ms: number): Promise<void> => {
      sleeps.push(ms);
      return Promise.resolve();
    };
    const errors: unknown[] = [];
    const owner = new KafkaPartitionOwner(client, () => undefined, {
      sleep: fakeSleep,
      retryBackoffMs: (attempt) => attempt * 10,
      onAcquireError: (_i, err) => errors.push(err),
    });

    owner.setWanted([0]);
    // Let the failed attempts and their (faked, instant) backoff sleeps run
    // to completion — each is a chain of microtasks/promise resolutions.
    for (let i = 0; i < 10; i++) await Promise.resolve();

    expect(attempts).toBe(3);
    expect(errors).toHaveLength(2);
    expect(sleeps).toEqual([0, 10]); // backoff(attempt 0), backoff(attempt 1)
    expect(owner.currentlyOwned().has(0)).toBe(true);
  });

  it("does not retry a failed acquire once the partition is no longer wanted", async () => {
    // The retry's backoff sleep is gated on a promise the test controls, so
    // exactly one acquire attempt fires before the test decides what happens
    // next — unlike an always-resolved fake sleep, this can't race ahead of
    // the test's own assertions and spin retries forever in the background.
    let attempts = 0;
    const backoffGate = deferred<void>();
    const client: PartitionOwnershipClient = {
      acquire: async () => {
        attempts++;
        throw new Error("cursor store blip");
      },
      release: () => undefined,
    };
    const owner = new KafkaPartitionOwner(client, () => undefined, {
      sleep: () => backoffGate.promise,
    });

    owner.setWanted([0]);
    await Promise.resolve();
    await Promise.resolve();
    expect(attempts).toBe(1);

    owner.setWanted([]); // no longer wanted before the retry's backoff elapses
    backoffGate.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(attempts).toBe(1); // never retried
  });

  it("stop() halts further retries", async () => {
    let attempts = 0;
    const backoffGate = deferred<void>();
    const client: PartitionOwnershipClient = {
      acquire: async () => {
        attempts++;
        throw new Error("cursor store blip");
      },
      release: () => undefined,
    };
    const owner = new KafkaPartitionOwner(client, () => undefined, {
      sleep: () => backoffGate.promise,
    });

    owner.setWanted([0]);
    await Promise.resolve();
    await Promise.resolve();
    expect(attempts).toBe(1);

    owner.stop();
    backoffGate.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    expect(attempts).toBe(1);
  });

  it("stop() disarms a pending retry backoff timer instead of leaving it armed", async () => {
    vi.useFakeTimers();
    try {
      const client: PartitionOwnershipClient = {
        acquire: async () => {
          throw new Error("cursor store blip");
        },
        release: () => undefined,
      };
      const owner = new KafkaPartitionOwner(client, () => undefined, {
        retryBackoffMs: () => 5000,
        onAcquireError: () => undefined,
      });

      owner.setWanted([0]);
      await vi.advanceTimersByTimeAsync(0);
      expect(vi.getTimerCount()).toBe(1); // the retry backoff

      owner.stop();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
