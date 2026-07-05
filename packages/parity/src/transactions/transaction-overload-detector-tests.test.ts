// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/TransactionOverloadDetectorTests.cs @ v10.1.0 (MIT).
//
// TransactionOverloadDetectorTests.RateLimitTest is a [SkippableTheory] with 3
// InlineData cases, each spinning a 60-second busy loop against a real
// `ITransactionOverloadDetector`/`TransactionRateLoadSheddingOptions`
// (src/Orleans.Transactions/TransactionOverloadDetector.cs and
// TransactionRateLoadSheddingOptions.cs) and asserting the observed admission
// rate lands within +/-10% of the configured limit.
//
// This framework has no equivalent transaction-rate load-shedding detector at
// all (no `ITransactionOverloadDetector`, `TransactionAgentStatistics` or
// `TransactionRateLoadSheddingOptions` counterpart exists in
// @tsva/transactions or @tsva/runtime) — a missing feature, not a bug to route
// around. Independently, the upstream test's mechanism — a 60-second
// wall-clock busy loop measuring real throughput — could not be ported
// faithfully under this repo's "no real sleeps > 1s" rule even if the
// detector existed, since it measures actual achieved rate rather than
// firing off virtual-clock timers a FakeTimeProvider could fast-forward.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-TRANSACTION-OVERLOAD-DETECTOR",
  "Orleans.Transactions.Tests.TransactionOverloadDetectorTests.RateLimitTest",
);

// vitest requires at least one runtime test per file; the only upstream Fact
// is gapped above, so this placeholder keeps the file a valid suite.
it.skip("(the only test in this file is orleansTest.gap — see above)", () => undefined);
