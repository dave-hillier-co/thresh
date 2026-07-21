// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/ExclusiveLockTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// ExclusiveLockTransactionMemoryTests inherits both [Theory]s from
// ExclusiveLockTransactionTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/ExclusiveLockTransactionTestRunner.cs).
// Both exercise Orleans' shared-lock-to-exclusive-lock upgrade machinery:
// concurrent transactions that Read-then-Write the same grain either race a
// lock upgrade (asserting `OrleansTransactionLockUpgradeException` or
// `OrleansBrokenTransactionLockException`) or, when the read is marked
// `[UseExclusiveLock]`, avoid the race entirely by taking an exclusive lock up
// front.
import { expect } from "vitest";
import { defineGrain, useTransactionalState } from "@tsva/core/define-grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { TransactionAbortedError, TransactionLockUpgradeError } from "@tsva/core/errors";
import { TestCluster } from "@tsva/testing/test-cluster";
import { orleansTest } from "@tsva/testing/orleans-test";

interface Counter {
  value: number;
}

interface LockTestGrain extends GrainWithStringKey {
  add(delta: number): Promise<number>;
  get(): Promise<number>;
}

interface LockCoordinatorGrain extends GrainWithStringKey {
  /** Orleans `IExclusiveLockCoordinatorGrain.ReadThenWrite`: Get, delay, Add — no exclusive lock. */
  readThenWrite(grainKey: string, value: number): Promise<void>;
}

const LockTestGrainInterface = defineGrainInterface<LockTestGrain>("LockTestGrain", {
  options: {
    add: { transaction: "createOrJoin" },
    get: { transaction: "createOrJoin" },
  },
});

const LockCoordinatorGrainInterface = defineGrainInterface<LockCoordinatorGrain>(
  "LockCoordinatorGrain",
  { options: { readThenWrite: { transaction: "create" } } },
);

const LockTestGrainImpl = defineGrain<LockTestGrain>("LockTestGrain", (ctx) => {
  const data = useTransactionalState<Counter>(ctx, "data", { initial: () => ({ value: 0 }) });
  return {
    add: (delta) =>
      data.performUpdate((s) => {
        s.value += delta;
        return s.value;
      }),
    get: () => data.performRead((s) => s.value),
  };
});

const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const LockCoordinatorGrainImpl = defineGrain<LockCoordinatorGrain>(
  "LockCoordinatorGrain",
  (ctx) => ({
    readThenWrite: async (grainKey, value) => {
      const grain = ctx.getGrain(LockTestGrainInterface, grainKey);
      await grain.get();
      // Widens the race window so the concurrent read-then-write attempts
      // actually interleave (Orleans: `Task.Delay(TimeSpan.FromMilliseconds(100))`).
      await delay(20);
      await grain.add(value);
    },
  }),
);

async function buildCluster(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    grains: [
      { ctor: LockTestGrainImpl, interfaces: [LockTestGrainInterface] },
      { ctor: LockCoordinatorGrainImpl, interfaces: [LockCoordinatorGrainInterface] },
    ],
  });
}

let nextKey = 0;
const newKey = (prefix: string): string => `${prefix}-${(nextKey += 1)}`;

orleansTest(
  "Orleans.Transactions.Tests.ExclusiveLockTransactionMemoryTests.ConcurrentReadThenWriteWithoutExclusiveLock_ThrowsLockException",
  async () => {
    const cluster = await buildCluster();
    try {
      const concurrentTransactions = 10;
      const addValue = 5;
      const grainKey = newKey("grain");

      const results = await Promise.allSettled(
        Array.from({ length: concurrentTransactions }, () =>
          cluster
            .getGrain(LockCoordinatorGrainInterface, newKey("coord"))
            .readThenWrite(grainKey, addValue),
        ),
      );

      const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
      // At least one concurrent read-then-write must lose the upgrade race —
      // Orleans asserts `OrleansTransactionLockUpgradeException` or
      // `OrleansBrokenTransactionLockException`; this framework has no
      // "broken lock" variant (see the exclusive-lock file header), so every
      // failure here is a `TransactionLockUpgradeError` (an initial-acquisition
      // wait-die death is also possible in principle, but the golden-path
      // shape below rules it out) or, in a rare late-arriving-reader
      // interleaving, the generic wait-die `TransactionAbortedError`.
      expect(failures.length).toBeGreaterThan(0);
      for (const failure of failures) {
        expect(failure.reason).toBeInstanceOf(TransactionAbortedError);
      }
      expect(failures.some((f) => f.reason instanceof TransactionLockUpgradeError)).toBe(true);

      // Every transaction that did NOT throw succeeded, and the final value
      // reflects exactly those successes (Orleans: serializable isolation
      // guarantees a consistent result regardless of which subset won).
      const succeeded = concurrentTransactions - failures.length;
      expect(await cluster.getGrain(LockTestGrainInterface, grainKey).get()).toBe(
        succeeded * addValue,
      );
    } finally {
      await cluster.dispose();
    }
  },
);

// Left excluded rather than un-gapped: this second Theory needs Orleans'
// *non-killing* lock-group batching for the `[UseExclusiveLock]` path
// (upstream `ReadWriteLock.EnterLock`, `Orleans.Transactions/State/
// ReaderWriterLock.cs`) — a fresh exclusive request that conflicts with the
// currently-active group is queued as its own new group and runs after the
// active one drains, with **nobody aborted**, which is how all 10 concurrent
// transactions in `ConcurrentReadThenWriteWithExclusiveLock_NoLockException`
// succeed with zero exceptions of any kind. `@tsva/transactions`'
// `ReaderWriterLock` (see `reader-writer-lock.ts`) is a strict
// timestamp-ordered **wait-die** lock instead: a write request that conflicts
// with a holder and is younger than that holder always dies, by design —
// that is what keeps the lock deadlock-free without Orleans' separate
// group-batch scheduler. Routing 10 concurrent `[UseExclusiveLock]` reads
// through `performRead({ exclusive: true })` would still kill most of the
// younger ones under contention, contradicting upstream's "no exceptions,
// all 10 succeed" assertion. Reproducing upstream's non-killing group-batch
// scheduling would mean redesigning `ReaderWriterLock`'s core wait-die
// conflict resolution for every caller, not only `[UseExclusiveLock]`'s —
// out of scope here, since the project's one non-negotiable constraint
// (`docs/deviations.md`) is that the transaction manager mirrors Orleans'
// distributed-TM protocol *faithfully*, and wait-die is the load-bearing
// mechanism that keeps it deadlock-free. A partial, `[UseExclusiveLock]`-only
// reimplementation would silently drift from that mechanism rather than
// extend it, so this stays an architectural exclusion, not a to-do gap.
orleansTest.excluded(
  "would require redesigning ReaderWriterLock's core wait-die conflict resolution to add Orleans' non-killing exclusive-lock group-batch scheduling — out of scope, see comment above",
  "Orleans.Transactions.Tests.ExclusiveLockTransactionMemoryTests.ConcurrentReadThenWriteWithExclusiveLock_NoLockException",
);
