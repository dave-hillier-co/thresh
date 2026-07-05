// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/TocFaultTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// TocFaultTransactionMemoryTests inherits its two [Theory]s from
// TocFaultTransactionTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/TocFaultTransactionTestRunner.cs).
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// MultiGrainWriteTransactionWithCommitFailure carries
// `[SkippableTheory(Skip = "https://github.com/dotnet/orleans/issues/9556")]`
// upstream — permanently disabled pending that issue, like the sibling
// TocGoldenPathMemoryTests.MultiGrainWriteTransaction (see
// toc-golden-path-memory-tests.test.ts).
orleansTest.excluded(
  "skipped upstream: SkippableTheory(Skip) pending dotnet/orleans#9556",
  "Orleans.Transactions.Tests.TocFaultTransactionMemoryTests.MultiGrainWriteTransactionWithCommitFailure",
);

// MultiGrainWriteTransactionWithCommitException does run upstream. It
// exercises an `ITransactionCommitterTestGrain` acting as a non-grain "commit
// service" resource (RemoteCommitService.cs) enlisted directly in the 2PC
// commit phase, and asserts the failure surfaces as
// `OrleansTransactionInDoubtException` (src/Orleans.Transactions/
// OrleansTransactionException.cs) — the "commit outcome unknown because the
// resource threw during commit, after prepare succeeded" case. This
// framework's `TransactionAgent` (@tsva/runtime/transaction-agent) drives only
// grain-hosted `TransactionParticipant`s (@tsva/core/transaction-info) through
// prepare/commit/abort; there is no non-grain external-resource commit
// abstraction, and no in-doubt-vs-aborted exception distinction (only the
// generic `TransactionAbortedError`). Both are missing features, not bugs.
orleansTest.gap(
  "GAP-TRANSACTION-EXCEPTION-TYPES",
  "Orleans.Transactions.Tests.TocFaultTransactionMemoryTests.MultiGrainWriteTransactionWithCommitException",
);

// vitest requires at least one runtime test per file; both upstream Facts are
// excluded/gapped above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded/gap — see above)", () => undefined);
