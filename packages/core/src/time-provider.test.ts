import { describe, expect, it } from "vitest";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { TimeProvider } from "@thresh/core/time-provider";
import { nowNanosOf, systemTimeProvider } from "@thresh/core/time-provider";

// .NET's `System.TimeProvider` pairs `GetUtcNow()` with a high-resolution
// `GetTimestamp()`/`TimestampFrequency`, and Orleans reads the fine one where a
// millisecond is too coarse (`ActivationRebalancerWorker`). `TimeProvider.now()`
// here is `Date.now()`-shaped, so a grain that mints ORDERED VALUES from the
// silo clock — SpaceDB's sequencer mints MVCC revisions as epoch nanoseconds —
// gets collisions at commit rate and has to bypass the clock to avoid them.

describe("systemTimeProvider.nowNanos", () => {
  it("resolves finer than a millisecond", () => {
    const readings: bigint[] = [];
    for (let i = 0; i < 2_000; i++) readings.push(systemTimeProvider.nowNanos!());

    // If the clock were `Date.now() * 1e6`, every reading would land on a whole
    // millisecond. This is the collision the sequencer hit.
    expect(readings.some((n) => n % 1_000_000n !== 0n)).toBe(true);

    const perMillisecond = new Map<bigint, Set<bigint>>();
    for (const n of readings) {
      const ms = n / 1_000_000n;
      const bucket = perMillisecond.get(ms) ?? new Set<bigint>();
      bucket.add(n);
      perMillisecond.set(ms, bucket);
    }
    // At least one millisecond held two readings that were told apart.
    expect([...perMillisecond.values()].some((bucket) => bucket.size > 1)).toBe(true);
  });

  it("never steps backwards", () => {
    let previous = systemTimeProvider.nowNanos!();
    for (let i = 0; i < 2_000; i++) {
      const next = systemTimeProvider.nowNanos!();
      expect(next >= previous).toBe(true);
      previous = next;
    }
  });

  it("reads the same wall clock as now(), in epoch nanoseconds", () => {
    const nanos = systemTimeProvider.nowNanos!();
    const ms = Number(nanos / 1_000_000n);
    expect(Math.abs(ms - systemTimeProvider.now())).toBeLessThan(1_000);
  });
});

describe("FakeTimeProvider.nowNanos", () => {
  it("is driven by advance(), so an ordered value minted from it stays deterministic", () => {
    const time = new FakeTimeProvider();

    expect(time.nowNanos()).toBe(0n);
    time.advance(1_500);
    expect(time.nowNanos()).toBe(1_500_000_000n);
    expect(time.nowNanos()).toBe(BigInt(time.now()) * 1_000_000n);
  });

  // The exact caller #60 exists for mints MVCC revisions as epoch nanoseconds, so a fake driving
  // one is advanced to a REALISTIC epoch -- and ~1.8e18 is past float64's integer-exact range.
  // Scaling before the bigint widening (`BigInt(Math.round(current * 1e6))`) rounds: advanced to
  // 1_700_000_000_123 it yields ...123000064n, breaking the identity below and handing the
  // consumer a revision that is not the instant it asked for.
  it("stays exact at a realistic epoch instant", () => {
    const time = new FakeTimeProvider();
    time.advance(1_700_000_000_123);
    expect(time.nowNanos()).toBe(1_700_000_000_123_000_000n);
    expect(time.nowNanos()).toBe(BigInt(time.now()) * 1_000_000n);
  });

  it("does not move on its own between advances", () => {
    const time = new FakeTimeProvider();
    time.advance(7);
    expect(time.nowNanos()).toBe(time.nowNanos());
  });
});

describe("nowNanosOf", () => {
  it("uses the provider's high-resolution reading when it has one", () => {
    const time = new FakeTimeProvider();
    time.advance(3);
    expect(nowNanosOf(time)).toBe(3_000_000n);
  });

  it("falls back to scaling now() for a provider that predates nowNanos", () => {
    const millisecondOnly: TimeProvider = {
      now: () => 42,
      setTimer: () => 0,
      clearTimer: () => {},
    };
    expect(nowNanosOf(millisecondOnly)).toBe(42_000_000n);
  });
});
