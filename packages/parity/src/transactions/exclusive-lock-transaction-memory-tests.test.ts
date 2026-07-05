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
//
// This framework now has both building blocks in isolation:
// `TransactionLockUpgradeError` (@tsva/core/errors, thrown by
// `ReaderWriterLock.enter`'s read-to-write upgrade path — see
// reader-writer-lock.test.ts) and a `{ exclusive: true }` option on
// `TransactionalState.performRead` that takes a write lock up front instead
// of a shared read lock (see transactional-state-impl.test.ts's two
// "read-then-write race" tests, which reproduce exactly the upgrade-collision
// and exclusive-lock-avoids-it shapes at the lock/state level,
// deterministically, without a cluster).
//
// What remains genuinely missing — and is *not* safe to fake — is Orleans'
// non-killing lock-group batching for the exclusive-lock path itself.
// Upstream's `ReadWriteLock.EnterLock` does not resolve an
// [UseExclusiveLock] contention by wait-die (older waits, younger dies): a
// fresh exclusive request that conflicts with the currently-active group is
// simply queued as its own *new* group and runs after the active one drains,
// with nobody aborted — which is how all `concurrentTransactions` in
// `ConcurrentReadThenWriteWithExclusiveLock_NoLockException` succeed with
// zero exceptions of *any* kind. `@tsva/transactions`' `ReaderWriterLock` is
// a strict timestamped wait-die lock: a write request that conflicts with a
// holder and is *younger* than that holder always dies (by design — that's
// what keeps it deadlock-free), so routing 10 concurrent transactions'
// `[UseExclusiveLock]` read through `performRead({ exclusive: true })` would
// still kill most of the younger ones under contention, contradicting
// upstream's "no exceptions, all 10 succeed" assertion. Reproducing upstream's
// non-killing group-batch scheduling would mean redesigning
// `ReaderWriterLock`'s core wait-die conflict resolution — explicitly out of
// scope for this task (existing transaction-concurrency correctness is
// paramount) and not attempted here. Left gapped rather than landing a test
// that either flakes or silently drifts from the real upstream semantics.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTION-EXCLUSIVE-LOCK",
  "Orleans.Transactions.Tests.ExclusiveLockTransactionMemoryTests.ConcurrentReadThenWriteWithoutExclusiveLock_ThrowsLockException",
);
orleansTest.gap(
  "GAP-TRANSACTION-EXCLUSIVE-LOCK",
  "Orleans.Transactions.Tests.ExclusiveLockTransactionMemoryTests.ConcurrentReadThenWriteWithExclusiveLock_NoLockException",
);

// vitest requires at least one runtime test per file; both upstream Facts are
// gapped above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
