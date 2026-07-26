// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/TocGoldenPathMemoryTests.cs @ v10.1.0 (MIT).
//
// TocGoldenPathTransactionMemoryTests inherits its single test method from
// TocGoldenPathTestRunnerxUnit (src/Orleans.Transactions.TestKit.xUnit/TOCGoldenPathTestRunner.cs),
// whose override carries `[SkippableTheory(Skip = "https://github.com/dotnet/orleans/issues/9556")]` —
// it is permanently disabled upstream pending that issue and never runs
// against the real Orleans transaction commit-service (TOC) protocol.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

orleansTest.excluded(
  "skipped upstream: SkippableTheory(Skip) pending dotnet/orleans#9556",
  "Orleans.Transactions.Tests.TocGoldenPathTransactionMemoryTests.MultiGrainWriteTransaction",
);

// vitest requires at least one runtime test per file; the only upstream Fact
// is excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
