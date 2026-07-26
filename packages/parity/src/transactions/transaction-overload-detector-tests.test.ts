// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/TransactionOverloadDetectorTests.cs @ v10.1.0 (MIT).
//
// Upstream's `RateLimitTest` spins a 60-second wall-clock busy loop against a
// real `TransactionOverloadDetector`, offering a transaction start every
// iteration and admitting it only when the detector reports it is not
// overloaded, then asserts the achieved admission rate lands within +/-10% of
// the configured limit. The detector is entirely clock-driven (a decaying
// transactions-per-second estimate snapshotted every 15s — see
// `@thresh/transactions/transaction-overload-detector`), so it reproduces
// deterministically under a `FakeTimeProvider`: the busy loop is simulated by
// advancing virtual time a fixed slice per iteration, offering far more
// opportunities per second than any limit so the detector — not the loop — is
// the throttle. No real time passes.
import { describe, expect } from "vitest";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import {
  DEFAULT_TRANSACTION_RATE_LIMIT,
  TransactionAgentStatisticsImpl,
  TransactionOverloadDetector,
} from "@thresh/transactions/transaction-overload-detector";
import { orleansTest } from "@thresh/testing/orleans-test";

// One iteration of virtual time. At 0.1ms/iteration the loop offers 10,000
// starts per simulated second — well above every limit under test, so the
// detector caps the achieved rate rather than the loop's speed.
const ITERATION_MS = 0.1;

function measureAdmissionRate(runTimeSeconds: number, limit: number): number {
  const time = new FakeTimeProvider();
  const statistics = new TransactionAgentStatisticsImpl();
  const detector = new TransactionOverloadDetector(statistics, { enabled: true, limit }, time);

  const runTimeMs = runTimeSeconds * 1000;
  while (time.now() < runTimeMs) {
    if (!detector.isOverloaded()) {
      statistics.trackTransactionStarted();
    }
    time.advance(ITERATION_MS);
  }

  // Orleans: averageRate = TransactionsStarted * 1000 / elapsedMs.
  return (statistics.transactionsStarted * 1000) / runTimeMs;
}

describe("Orleans.Transactions.Tests.TransactionOverloadDetectorTests", () => {
  orleansTest.each([
    [60, DEFAULT_TRANSACTION_RATE_LIMIT],
    [60, DEFAULT_TRANSACTION_RATE_LIMIT * 2],
    [60, 1],
  ] as const)(
    "Orleans.Transactions.Tests.TransactionOverloadDetectorTests.RateLimitTest",
    ([runTimeSeconds, limit]) => {
      const averageRate = measureAdmissionRate(runTimeSeconds, limit);
      // Within +/-10% of the configured limit (Orleans assertion).
      expect(averageRate).toBeGreaterThanOrEqual(limit * 0.9);
      expect(averageRate).toBeLessThanOrEqual(limit * 1.1);
    },
  );
});
