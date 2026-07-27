// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/CountersGrainPerfTests.cs @ v10.1.0 (MIT).
// Upstream is "not really a test, just an illustration of how JournaledGrain
// performance can vary with the choices made" (confirm-each-update vs.
// confirm-at-end, reentrant vs. non-reentrant, state-store vs. log-store
// consistency provider) — it asserts no numbers, just prints elapsed time.
// This framework has one journal-storage substrate (no separate "persist
// latest state only" vs. "persist the full log" providers, and no artificial
// storage-latency simulation), so the four grain variants below differ only
// in reentrancy; each test still exercises the same functional protocol
// (`ConcurrentIncrementsRunner`) the upstream perf tests share with
// `CountersGrainTests`, just without timing assertions. `iterations` is
// smaller than upstream's 800 to keep this fast in CI; it only has to be
// enough to make concurrent-raise ordering exercise the log meaningfully.
import { afterAll, beforeAll, describe } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  CountersGrainLogStoreNonReentrant,
  CountersGrainLogStoreReentrant,
  CountersGrainStateStoreNonReentrant,
  CountersGrainStateStoreReentrant,
  ICountersGrainLogStoreNonReentrant,
  ICountersGrainLogStoreReentrant,
  ICountersGrainStateStoreNonReentrant,
  ICountersGrainStateStoreReentrant,
} from "@thresh/parity/grains/impl/counters-grain";
import { concurrentIncrementsRunner } from "@thresh/parity/event-sourcing/counters-runner";

const iterations = 100;

describe("Tester.EventSourcingTests.CountersGrainTests.Perf", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        {
          ctor: CountersGrainStateStoreNonReentrant,
          interfaces: [ICountersGrainStateStoreNonReentrant],
        },
        { ctor: CountersGrainStateStoreReentrant, interfaces: [ICountersGrainStateStoreReentrant] },
        {
          ctor: CountersGrainLogStoreNonReentrant,
          interfaces: [ICountersGrainLogStoreNonReentrant],
        },
        { ctor: CountersGrainLogStoreReentrant, interfaces: [ICountersGrainLogStoreReentrant] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("Tester.EventSourcingTests.CountersGrainTests.Perf_Warmup", async () => {
    // call reset on each grain to ensure everything is loaded and primed
    await Promise.all([
      cluster.getGrain(ICountersGrainStateStoreNonReentrant, 0n).reset(true),
      cluster.getGrain(ICountersGrainStateStoreReentrant, 0n).reset(true),
      cluster.getGrain(ICountersGrainLogStoreNonReentrant, 0n).reset(true),
      cluster.getGrain(ICountersGrainLogStoreReentrant, 0n).reset(true),
    ]);
  });

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryStateStore_NonReentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainStateStoreNonReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, true);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryStateStore_NonReentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainStateStoreNonReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, false);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryLogStore_NonReentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainLogStoreNonReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, true);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryLogStore_NonReentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainLogStoreNonReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, false);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryStateStore_Reentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainStateStoreReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, true);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryStateStore_Reentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainStateStoreReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, false);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmEachUpdate_MemoryLogStore_Reentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainLogStoreReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, true);
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.CountersGrainTests.Perf_ConfirmAtEndOnly_MemoryLogStore_Reentrant",
    async () => {
      const g = cluster.getGrain(ICountersGrainLogStoreReentrant, 0n);
      await concurrentIncrementsRunner(g, iterations, false);
    },
  );
});
