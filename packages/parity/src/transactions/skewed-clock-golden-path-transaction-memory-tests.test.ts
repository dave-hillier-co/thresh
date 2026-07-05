// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/SkewedClockGoldenPathTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// SkewedClockGoldenPathTransactionMemoryTests inherits every test method from
// the same GoldenPathTransactionTestRunnerxUnit as GoldenPathTransactionMemoryTests
// (see golden-path-transaction-memory-tests.test.ts), and every override there
// carries `[SkippableTheory(Skip = "https://github.com/dotnet/orleans/issues/9553")]`.
// The only difference from the plain Memory variant is the fixture's skewed
// clock configurator, which never runs because every test method is disabled.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const SKIP_REASON = "skipped upstream: SkippableTheory(Skip) pending dotnet/orleans#9553";

orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.SingleGrainReadTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.SingleGrainWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.MultiGrainWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.MultiGrainReadWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.RepeatGrainReadWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.MultiWriteToSingleGrainTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.RWRWTest",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.SkewedClockGoldenPathTransactionMemoryTests.WRWRTest",
);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
