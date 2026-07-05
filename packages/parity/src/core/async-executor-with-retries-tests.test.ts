// Ported from dotnet/orleans test/Orleans.Core.Tests/Async_AsyncExecutorWithRetriesTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Tests Orleans.Internal.AsyncExecutorWithRetries, a standalone, generically reusable
// retry-with-backoff utility (configurable max retries, error/success filter predicates,
// FixedBackoff strategy). This framework has no equivalent general-purpose composable retry
// primitive exposed as a public API - retry/backoff logic here is embedded directly inside the
// specific components that need it (e.g. shard-executor, transaction-agent, queue-pulling-agent)
// rather than factored into one reusable executor to unit test in isolation. This is a genuine
// feature gap (a portable, non-.NET-specific algorithm), not an architectural exclusion, so every
// case is tracked as a gap rather than excluded.
describe("NonSilo.Tests.Async_AsyncExecutorWithRetriesTests", () => {
  orleansTest.gap(
    "GAP-RETRY-EXECUTOR",
    "NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_1",
  );
  orleansTest.gap(
    "GAP-RETRY-EXECUTOR",
    "NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_2",
  );
  orleansTest.gap(
    "GAP-RETRY-EXECUTOR",
    "NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_4",
  );
  orleansTest.gap(
    "GAP-RETRY-EXECUTOR",
    "NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_5",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // gapped above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.gap - see above)", () => undefined);
});
