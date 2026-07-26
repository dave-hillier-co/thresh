// Ported from dotnet/orleans test/Orleans.Core.Tests/SchedulerTests/STSchedulerLongTurnTest.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Verifies that Orleans' per-silo thread pool (16 threads across two silos) doesn't queue up
// long-running (12s sleeping) grain calls behind each other and time out, by swamping 100
// grains with sleeping requests and asserting completion under 40s wall-clock. This depends on
// Orleans' specific multi-threaded per-silo scheduler sizing, which has no counterpart in this
// framework's single-threaded event-loop concurrency model, and it requires real multi-second
// sleeps forbidden by this port's no-real-sleeps-over-1s rule.
describe("DefaultCluster.Tests.SchedulerTests.STSchedulerLongTurnTest", () => {
  orleansTest.excluded(
    ".NET-specific: depends on Orleans' per-silo multi-threaded scheduler sizing (16 threads) not queuing long-running turns; this framework has no per-silo thread pool to swamp, and the test requires real multi-second sleeps",
    "DefaultCluster.Tests.SchedulerTests.STSchedulerLongTurnTest.Sched_LongTurnTest",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
