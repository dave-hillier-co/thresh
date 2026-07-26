// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/GoldenPathTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// GoldenPathTransactionMemoryTests inherits every test method from
// GoldenPathTransactionTestRunnerxUnit (src/Orleans.Transactions.TestKit.xUnit/GoldenPathTransactionTestRunner.cs),
// where all eight [Theory] overrides carry
// `[SkippableTheory(Skip = "https://github.com/dotnet/orleans/issues/9553")]` —
// i.e. every one of these tests is permanently disabled upstream pending that
// issue. None of them ever runs against the real Orleans transaction system
// either, so there is nothing behavioural to port.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const SKIP_REASON = "skipped upstream: SkippableTheory(Skip) pending dotnet/orleans#9553";

orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.SingleGrainReadTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.SingleGrainWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.MultiGrainWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.MultiGrainReadWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.RepeatGrainReadWriteTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.MultiWriteToSingleGrainTransaction",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.RWRWTest",
);
orleansTest.excluded(
  SKIP_REASON,
  "Orleans.Transactions.Tests.GoldenPathTransactionMemoryTests.WRWRTest",
);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
