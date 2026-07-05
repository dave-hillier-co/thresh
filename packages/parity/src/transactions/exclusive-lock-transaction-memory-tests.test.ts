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
// front. This framework's wait-die lock (@tsva/transactions) has no
// shared/exclusive lock distinction or `[UseExclusiveLock]`-equivalent
// annotation, and no equivalent lock-upgrade/broken-lock exception types —
// only a generic `TransactionAbortedError`. Both are missing features.
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
