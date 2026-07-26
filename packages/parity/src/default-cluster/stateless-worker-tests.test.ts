// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/StatelessWorkerTests.cs @ v10.1.0 (MIT).
//
// One case needs a capability this framework does not have: (for the
// constructor-throw case) .NET's specific "Failed to create an instance of
// grain type" wrapping with an `InnerException`, which this framework's
// plain activation-error propagation does not replicate.
//
// The 3 `[MayInterleave]` cases are different: upstream's grain under test
// (`StatelessWorkerWithMayInterleaveGrain`) is marked `[StatelessWorker(1)]`
// purely to force every call onto a SINGLE local activation — see that grain's
// own source comment, quoted in may-interleave-grain.ts. Interleaving is a
// per-activation `TurnScheduler` concern, not a multi-activation placement
// one, and this framework already activates every grain as a single
// activation by default — so these 3 are un-gapped onto a plain
// `@mayInterleave()` grain rather than left blocked on GAP-STATELESS-WORKER.
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { randomIntegerKey } from "@thresh/parity/support/keys";
import {
  IMayInterleaveGrain,
  MayInterleaveGrain,
  mayInterleaveCallStarted,
  mayInterleaveStartedTags,
  releaseMayInterleaveCalls,
  resetMayInterleaveTestState,
} from "@thresh/parity/grains/impl/may-interleave-grain";
import {
  IStatelessWorkerGrain,
  StatelessWorkerGrain,
} from "@thresh/parity/grains/impl/stateless-worker-grain";

/** Flush pending microtasks/macrotasks so any admitted-but-not-yet-observed turn gets a chance to start. */
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("DefaultCluster.Tests.General.StatelessWorkerTests", () => {
  orleansTest.excluded(
    '.NET-specific activation-failure wrapping: "Failed to create an instance of grain type" ' +
      "with a nested InnerException has no analogue in this framework's plain constructor-error propagation",
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorkerThrowExceptionConstructor",
  );

  orleansTest.excluded(
    'skipped upstream: "Skipping test for now, since there seems to be a bug" (SkippableFact Skip=...)',
    "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorkerFastActivationsDontFailInMultiSiloDeployment",
  );

  describe("[StatelessWorker]", () => {
    // Upstream `StatelessWorkerGrain.MaxLocalWorkers` (the grain's own
    // `[StatelessWorker(1)]` declaration).
    const ExpectedMaxLocalActivations = 1;
    let cluster: TestCluster;

    beforeAll(async () => {
      cluster = await TestCluster.start({
        initialSilos: 1,
        grains: [{ ctor: StatelessWorkerGrain, interfaces: [IStatelessWorkerGrain] }],
      });
    });

    afterAll(async () => {
      await cluster.dispose();
    });

    orleansTest(
      "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorkerActivationsPerSiloDoNotExceedMaxLocalWorkersCount",
      async () => {
        // do extra calls to trigger activation of ExpectedMaxLocalActivations local activations
        const numberOfCalls = ExpectedMaxLocalActivations * 3;
        const grain = cluster.getGrain(IStatelessWorkerGrain, randomIntegerKey());

        // warmup
        await grain.longCall();
        await new Promise((r) => setTimeout(r, 50));

        const calls: Array<Promise<void>> = [];
        for (let i = 0; i < numberOfCalls; i += 1) calls.push(grain.longCall());
        await Promise.all(calls);

        const statsTasks: Array<Promise<[string, string, Array<[number, number]>]>> = [];
        for (let i = 0; i < numberOfCalls; i += 1) statsTasks.push(grain.getCallStats());
        const responses = await Promise.all(statsTasks);

        const activationsPerSilo = new Map<string, Set<string>>();
        for (const [activationGuid, silo] of responses) {
          const activations = activationsPerSilo.get(silo) ?? new Set<string>();
          activations.add(activationGuid);
          activationsPerSilo.set(silo, activations);
        }
        for (const [silo, activations] of activationsPerSilo) {
          expect(
            activations.size,
            `silo ${silo} had ${activations.size} activations but expected no more than ${ExpectedMaxLocalActivations}`,
          ).toBeLessThanOrEqual(ExpectedMaxLocalActivations);
        }
      },
    );

    orleansTest(
      "DefaultCluster.Tests.General.StatelessWorkerTests.ManyConcurrentInvocationsOnActivationLimitedStatelessWorkerDoesNotFail",
      async () => {
        // Issue #6795: significantly more concurrent invocations than the local worker limit results in too many
        // message forwards. When the issue occurs, this test will throw an exception.
        //
        // We are trying to trigger a race condition and need more than 1 attempt to reliably reproduce the issue.
        for (let attempt = 0; attempt < 100; attempt += 1) {
          const grain = cluster.getGrain(IStatelessWorkerGrain, BigInt(attempt));
          await Promise.all(Array.from({ length: 10 }, () => grain.dummyCall()));
        }
      },
    );
  });

  describe("[MayInterleave]", () => {
    let cluster: TestCluster;

    beforeAll(async () => {
      cluster = await TestCluster.start({
        initialSilos: 1,
        grains: [{ ctor: MayInterleaveGrain, interfaces: [IMayInterleaveGrain] }],
      });
    });

    afterAll(async () => {
      await cluster.dispose();
    });

    beforeEach(() => {
      resetMayInterleaveTestState();
    });

    orleansTest(
      "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorker_DoesNotThrow_IfMarkedWithMayInterleave",
      async () => {
        const grain = cluster.getGrain(IMayInterleaveGrain, randomIntegerKey());
        const task = grain.goFast("go-fast");
        await mayInterleaveCallStarted("go-fast");
        releaseMayInterleaveCalls();
        await expect(task).resolves.toBeNull();
      },
    );

    orleansTest(
      "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorker_ShouldNotInterleaveCalls_IfMayInterleavePredicatedDoesntMatch",
      async () => {
        const grain = cluster.getGrain(IMayInterleaveGrain, randomIntegerKey());
        const tags = ["slow-1", "slow-2", "slow-3"];
        const completions = tags.map((tag) => grain.goSlow(tag));
        const starts = tags.map((tag) => mayInterleaveCallStarted(tag));

        // Exactly one call is admitted and starts running...
        await Promise.race(starts);
        await flush();
        await flush();
        // ...and, unlike the matching-predicate case below, the other two stay
        // queued behind it rather than interleaving.
        expect(mayInterleaveStartedTags().size).toBe(1);

        releaseMayInterleaveCalls();
        await Promise.all(completions);
        // All three eventually run, one after another.
        expect(mayInterleaveStartedTags().size).toBe(3);
      },
    );

    orleansTest(
      "DefaultCluster.Tests.General.StatelessWorkerTests.StatelessWorker_ShouldInterleaveCalls_IfMayInterleavePredicatedMatches",
      async () => {
        const grain = cluster.getGrain(IMayInterleaveGrain, randomIntegerKey());
        const tags = ["fast-1", "fast-2", "fast-3"];
        const completions = tags.map((tag) => grain.goFast(tag));

        // All three are admitted concurrently: none is queued behind another.
        await Promise.all(tags.map((tag) => mayInterleaveCallStarted(tag)));
        expect(mayInterleaveStartedTags().size).toBe(3);

        releaseMayInterleaveCalls();
        await Promise.all(completions);
      },
    );
  });
});
