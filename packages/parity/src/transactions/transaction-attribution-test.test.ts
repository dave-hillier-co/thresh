// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/TransactionAttributionTest.cs @ v10.1.0 (MIT).
//
// TransactionAttributionTest inherits every [Fact] from
// TransactionAttributionTestRunner
// (test/Transactions/Orleans.Transactions.Tests/Runners/
// TransactionAttributionTestRunner.cs), which drives six grain variants — one
// per `[Transaction(TransactionOption...)]` value (no attribute, Suppress,
// CreateOrJoin, Create, Join, Supported, NotAllowed) — through
// `ITransactionAttributionGrain.GetNestedTransactionIds`, a recursive call
// tree that has each tier read `TransactionContext.GetTransactionInfo()?.Id`
// and report it back up, so the test can assert which tiers see no ambient
// transaction, which see a fresh one, and which see the parent's.
//
// This framework's `InvokeMethodOptions.transaction` (@tsva/core/invoke-options)
// and `resolveTransaction` (@tsva/runtime/grain-factory) do implement the same
// create/createOrJoin/join/supported/notAllowed/suppress boundary semantics —
// but there is no public way for grain application code to read "what
// transaction (if any) is this turn running inside". The ambient transaction
// lives in `@tsva/runtime`'s `invocationContext`/`currentTransaction()`, which
// is runtime-internal: `TransactionInfo` (@tsva/core/transaction-info),
// `GrainContext` (@tsva/core/grain-context) and `GrainRuntime`
// (@tsva/core/grain-runtime) — the only surfaces grain code in this package
// can reach — expose no transaction-id accessor at all. Every one of these
// tests hinges on that introspection primitive, so the whole file is a single
// missing-feature gap: not a test-kit-only gap, but an application-facing API
// that does not exist yet for any grain author to use.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.AllSupportedAttributesFromOutsideTransactionTest",
);
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.AllSupportedAttributesFromWithinTransactionTest",
);
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.CreateOrJoinTest",
);
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.CreateTest",
);
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.SupportedTest",
);
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.JoinFailTest",
);
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.NotAllowedFailTest",
);

// vitest requires at least one runtime test per file; every upstream Fact is
// gapped above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
