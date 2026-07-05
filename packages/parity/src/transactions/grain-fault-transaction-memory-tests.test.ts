// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/GrainFaultTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// GrainFaultTransactionMemoryTests inherits every [Theory] from
// GrainFaultTransactionTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/GrainFaultTransactionTestRunner.cs),
// each with 3 InlineData cases over TransactionTestConstants'
// Single/Double/MaxStateTransactionalGrain grain-state shapes. All five assert
// on specific members of Orleans' transaction exception hierarchy
// (src/Orleans.Transactions/OrleansTransactionException.cs):
// OrleansTransactionAbortedException, OrleansReadOnlyViolatedException,
// OrleansOrphanCallException and OrleansCascadingAbortException. This
// framework has a single generic `TransactionAbortedError`
// (@tsva/core/errors) with no distinct read-only-violation or orphan-call
// detection, and no equivalent of `AddAndThrowException` inner-exception
// unwrapping (.NET's `Exception.InnerException` chain has no TS analogue
// here). Faithfully asserting these upstream types is not possible without
// inventing new exception classes and detection logic that do not exist —
// this is a missing-feature gap, not a bug to work around.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTION-EXCEPTION-TYPES",
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionOnExceptions",
);
orleansTest.gap(
  "GAP-TRANSACTION-EXCEPTION-TYPES",
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionOnReadOnlyViolatedException",
);
orleansTest.gap(
  "GAP-TRANSACTION-EXCEPTION-TYPES",
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.MultiGrainAbortTransactionOnExceptions",
);
orleansTest.gap(
  "GAP-TRANSACTION-EXCEPTION-TYPES",
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionExceptionInnerExceptionOnlyContainsOneRootCauseException",
);
orleansTest.gap(
  "GAP-TRANSACTION-EXCEPTION-TYPES",
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionOnOrphanCalls",
);

// vitest requires at least one runtime test per file; every upstream Fact is
// gapped above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
