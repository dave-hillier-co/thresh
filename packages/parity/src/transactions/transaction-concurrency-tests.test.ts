// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/TransactionConcurrencyTests.cs @ v10.1.0 (MIT).
//
// TransactionConcurrencyTests inherits all three [Theory]s from
// TransactionConcurrencyTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/TransactionConcurrencyTestRunner.cs),
// each with 3 InlineData cases over the Single/Double/MaxStateTransactional
// grain shapes. Every override carries
// `[SkippableTheory(Skip = "https://github.com/dotnet/orleans/issues/9554")]`
// — all three are permanently disabled upstream pending that issue and never
// run against the real Orleans transaction concurrency machinery.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const SKIP_REASON = "skipped upstream: SkippableTheory(Skip) pending dotnet/orleans#9554";

orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.TransactionConcurrencyTests.SingleSharedGrainTest",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.TransactionConcurrencyTests.TransactionChainTest",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.TransactionConcurrencyTests.TransactionTreeTest",
);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
