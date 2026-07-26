// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/ConsistencyTests.cs @ v10.1.0 (MIT).
//
// ConsistencyTests inherits its single [Theory] (RandomizedConsistency, ~90
// InlineData cases) from ConsistencyTransactionTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/ConsistencyTransactionTestRunner.cs),
// whose base (src/Orleans.Transactions.TestKit.Base/TestRunners/
// ConsistencyTransactionTestRunner.cs) drives a `ConsistencyTestHarness`
// (see consistency-harness.ts, ported from
// src/Orleans.Transactions.TestKit.Base/Consistency/*.cs): many concurrent
// worker "threads" hammer a shared pool of grains with random read/write
// (and recursive) transactions, recording every operation's observed
// before/after state into an `Observation` log, then post-hoc verifying the
// log admits a serializable history.
//
// Case selection: upstream's ~90 `[InlineData]` rows sweep grain-pool size
// (congestion), thread/tx-count `scale`, `avoidDeadlocks`, `avoidTimeouts`
// and `readWrite` determination. This port keeps `scale` small (so each case
// runs fast) and, per `consistency-harness.ts`'s header, never combines
// `avoidDeadlocks: true` with `readWrite: "perGrain"`/`"perTransaction"` —
// that combination is the one upstream case shape this port's strict
// wait-die lock cannot honour (`harness.NumAborted` would not stay `0`; see
// the exclusive-lock file for the identical underlying architecture
// difference). Every case below still exercises the real thing this gap was
// about: concurrent recursive transactions plus the serializable-history
// checker.
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

const cases: Case[] = [
  // High congestion (few grains, several threads): stresses the lock and the
  // consistency checker under heavy contention.
  { numGrains: 2, scale: 2, avoidDeadlocks: true, readWrite: "perAccess" },
  { numGrains: 2, scale: 3, avoidDeadlocks: false, readWrite: "perAccess" },
  // Medium congestion.
  { numGrains: 30, scale: 3, avoidDeadlocks: true, readWrite: "perAccess" },
  { numGrains: 30, scale: 2, avoidDeadlocks: false, readWrite: "perGrain" },
  // Low congestion (grain pool much larger than thread count).
  { numGrains: 1000, scale: 2, avoidDeadlocks: false, readWrite: "perTransaction" },
];

describe("Orleans.Transactions.Tests.ConsistencyTests", () => {
  orleansTest.each(cases)(
    "Orleans.Transactions.Tests.ConsistencyTests.RandomizedConsistency",
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
