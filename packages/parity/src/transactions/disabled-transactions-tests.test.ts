// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Disabled/DisabledTransactionsTests.cs @ v10.1.0 (MIT).
//
// (The assignment also names Memory/DisabledTransactionsTests.cs, but no such
// file exists at v10.1.0 — only Disabled/DisabledTransactionsTests.cs does.)
//
// DisabledTransactionsTests uses DefaultClusterFixture, whose SiloHostConfigurator
// never calls `.UseTransactions()`, so any grain call carrying a `[Transaction]`
// attribute throws OrleansTransactionsDisabledException. It inherits both its
// [Theory] methods, undecorated by any Skip, from DisabledTransactionsTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/DisabledTransactionsTestRunner.cs).
//
// This framework has no equivalent "transactions not configured" mode: a silo
// built via @tsva/hosting always wires a default in-memory transactional-storage
// provider for `@transactionalState` facets (see silo-builder.ts), so there is
// no way to construct a cluster where a `[Transaction]`-style call throws for
// lack of a transaction manager. Ported as a gap rather than excluded, since the
// upstream behaviour (opt-in transactions) is a real, unimplemented feature.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTIONS-OPT-OUT",
  "Orleans.Transactions.Tests.DisabledTransactionsTests.TransactionGrainsThrowWhenTransactions",
);

orleansTest.gap(
  "GAP-TRANSACTIONS-OPT-OUT",
  "Orleans.Transactions.Tests.DisabledTransactionsTests.MultiTransactionGrainsThrowWhenTransactions",
);

// vitest requires at least one runtime test per file; both upstream Facts are
// gapped above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
