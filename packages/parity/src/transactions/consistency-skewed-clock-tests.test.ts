// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/ConsistencySkewedClockTests.cs @ v10.1.0 (MIT).
//
// ConsistencySkewedClockTests inherits the same single [Theory]
// (RandomizedConsistency) from the same ConsistencyTransactionTestRunnerxUnit
// as ConsistencyTests (see consistency-tests.test.ts) — only the fixture's
// clock-skew configurator differs, and the test body is identical. See
// consistency-tests.test.ts for why the underlying ConsistencyTestHarness
// (concurrent randomized workload generator + serializable-history checker)
// is a gap rather than a portable grain feature.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTION-CONSISTENCY-HARNESS",
  "Orleans.Transactions.Tests.ConsistencySkewedClockTests.RandomizedConsistency",
);

// vitest requires at least one runtime test per file; the only upstream Fact
// is gapped above, so this placeholder keeps the file a valid suite.
it.skip("(the only test in this file is orleansTest.gap — see above)", () => undefined);
