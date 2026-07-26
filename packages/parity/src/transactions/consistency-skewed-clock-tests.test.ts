// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/ConsistencySkewedClockTests.cs @ v10.1.0 (MIT).
//
// ConsistencySkewedClockTests inherits the same single [Theory]
// (RandomizedConsistency) from the same ConsistencyTransactionTestRunnerxUnit
// as ConsistencyTests (see consistency-tests.test.ts and
// consistency-harness.ts) — only the xUnit fixture's clock-skew configurator
// differs, and the test body is identical. That configurator injects an
// artificial per-silo clock offset to stress-test the causal clock's
// timestamp-merge behaviour across silos; against this port's single-silo
// `TestCluster` (matching the pattern used throughout this suite) there is no
// second silo's clock to skew against, so the scenario has no distinguishing
// effect here and this file exercises the identical case set as
// consistency-tests.test.ts, one silo's `CausalClock` being what upstream's
// skew ultimately exercises either way.
import { describe } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  ConsistencyTestGrainImpl,
  ConsistencyTestGrainInterface,
  runRandomizedConsistency,
  type ReadWriteDetermination,
} from "@thresh/parity/transactions/consistency-harness";

async function buildCluster(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    grains: [{ ctor: ConsistencyTestGrainImpl, interfaces: [ConsistencyTestGrainInterface] }],
  });
}

interface Case {
  numGrains: number;
  scale: number;
  avoidDeadlocks: boolean;
  readWrite: ReadWriteDetermination;
}

// See consistency-tests.test.ts for why these particular upstream case shapes
// were chosen (small `scale`, never `avoidDeadlocks: true` combined with
// `readWrite: "perGrain"`/`"perTransaction"`).
const cases: Case[] = [
  { numGrains: 2, scale: 2, avoidDeadlocks: true, readWrite: "perAccess" },
  { numGrains: 30, scale: 2, avoidDeadlocks: false, readWrite: "perGrain" },
  { numGrains: 1000, scale: 2, avoidDeadlocks: false, readWrite: "perTransaction" },
];

describe("Orleans.Transactions.Tests.ConsistencySkewedClockTests", () => {
  orleansTest.each(cases)(
    "Orleans.Transactions.Tests.ConsistencySkewedClockTests.RandomizedConsistency",
    async ({ numGrains, scale, avoidDeadlocks, readWrite }) => {
      const cluster = await buildCluster();
      try {
        await runRandomizedConsistency(cluster, numGrains, scale, avoidDeadlocks, readWrite);
      } finally {
        await cluster.dispose();
      }
    },
  );
});
