// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/ConsistencyTests.cs @ v10.1.0 (MIT).
//
// ConsistencyTests inherits its single [Theory] (RandomizedConsistency, ~90
// InlineData cases) from ConsistencyTransactionTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/ConsistencyTransactionTestRunner.cs),
// whose base (src/Orleans.Transactions.TestKit.Base/TestRunners/
// ConsistencyTransactionTestRunner.cs) drives a `ConsistencyTestHarness`
// (src/Orleans.Transactions.TestKit.Base/Consistency/ConsistencyTestHarness.cs):
// many concurrent worker "threads" hammer a shared pool of grains with random
// read/write transactions (including deliberate deadlock/timeout scenarios and
// injected storage errors), recording every operation's observed before/after
// state into an `Observation` log, then post-hoc verifying the log admits a
// serializable history (no committed transaction's reads/writes could have
// been produced by any other interleaving).
//
// This is a full randomized-testing harness, not a grain feature gap: it needs
// (a) a pool of many-state transactional test grains, (b) a coordinator
// exposing raw per-access read/write control so a workload generator can
// script arbitrary access patterns, and (c) a serializability checker over the
// recorded history. None of that test infrastructure exists in this framework
// yet, and building the checker faithfully (order-independent history
// verification, per Observation.cs) is itself a substantial feature, not
// something a grain author could work around. Ported as a single gap.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTION-CONSISTENCY-HARNESS",
  "Orleans.Transactions.Tests.ConsistencyTests.RandomizedConsistency",
);

// vitest requires at least one runtime test per file; the only upstream Fact
// is gapped above, so this placeholder keeps the file a valid suite.
it.skip("(the only test in this file is orleansTest.gap — see above)", () => undefined);
