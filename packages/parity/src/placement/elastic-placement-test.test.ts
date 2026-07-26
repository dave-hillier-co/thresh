// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/ElasticPlacementTest.cs @ v10.1.0 (MIT).
//
// The four elasticity tests are `[SkippableFact(Skip = "...")]` upstream
// (https://github.com/dotnet/orleans/issues/4008) — skipped there too. The
// remaining two (`LoadAwareGrainShouldNotAttemptToCreateActivationsOn*`) are
// ported below: this framework's load-aware placement strategy
// (`ActivationCountPlacement`) now excludes a silo `PlacementContext.isOverloaded`
// flags before sampling candidates (mirrors Orleans
// `ActivationCountPlacementDirector.SelectSiloPowerOfK`'s
// `if (localSiloStat.SiloStats.IsOverloaded) continue;`), and that flag is kept
// current across silos by `ClusterNode.publishLoadStats` — a forced push
// (mirroring Orleans' test-only `PropagateStatisticsToCluster`) run after every
// `IActivationCountBasedPlacementTestGrain` latch/unlatch call, so a peer silo's
// placement decision sees the change immediately rather than waiting on a
// periodic gossip interval this framework does not otherwise run. Setup routes
// through `getGrainAtSilo` (`@thresh/parity/support/get-grain-at-silo`), the port
// of upstream's `GetGrainAtSilo` helper, which pins a fresh grain onto a
// specific NEWLY STARTED silo via the `RequestContext`-driven placement hint
// (`IPlacementDirector.PlacementHintKey`, ex-GAP-REQUEST-CONTEXT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";
import {
  ActivationCountBasedPlacementTestGrain,
  IActivationCountBasedPlacementTestGrain,
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
  PreferLocalPlacementTestGrain,
  RandomPlacementTestGrain,
} from "@thresh/parity/grains/impl/placement-test-grain";
import { getGrainAtSilo } from "@thresh/parity/support/get-grain-at-silo";
import { randomGuidKey } from "@thresh/parity/support/keys";
import type { IPlacementTestGrain } from "@thresh/parity/grains/interfaces/placement-test-grain-interfaces";

const SAMPLE_SIZE = 10;

describe("UnitTests.General.ElasticPlacementTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: RandomPlacementTestGrain, interfaces: [IRandomPlacementTestGrain] },
        { ctor: PreferLocalPlacementTestGrain, interfaces: [IPreferLocalPlacementTestGrain] },
        {
          ctor: ActivationCountBasedPlacementTestGrain,
          interfaces: [IActivationCountBasedPlacementTestGrain],
        },
      ],
      loadShedding: { loadSheddingEnabled: true },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  /** Upstream `ElasticPlacementTests.ElasticityGrainPlacementTest`. */
  async function elasticityGrainPlacementTest(
    taint: (grain: IPlacementTestGrain) => Promise<void>,
    restore: (grain: IPlacementTestGrain) => Promise<void>,
  ): Promise<void> {
    const taintedSilo: TestSiloHandle = await cluster.startAdditionalSilo();
    const taintedGrain = await getGrainAtSilo(cluster, taintedSilo.address);

    try {
      await taint(taintedGrain);

      const places: string[] = [];
      for (let i = 0; i < SAMPLE_SIZE; i += 1) {
        const grain = cluster.getGrain(IActivationCountBasedPlacementTestGrain, randomGuidKey());
        places.push(await grain.getRuntimeInstanceId());
      }

      expect(places).not.toContain(taintedSilo.address.toString());
    } finally {
      await restore(taintedGrain);
    }
  }

  orleansTest(
    "UnitTests.General.ElasticPlacementTests.LoadAwareGrainShouldNotAttemptToCreateActivationsOnOverloadedSilo",
    async () => {
      await elasticityGrainPlacementTest(
        (g) => g.latchOverloaded(),
        (g) => g.unlatchOverloaded(),
      );
    },
  );

  orleansTest(
    "UnitTests.General.ElasticPlacementTests.LoadAwareGrainShouldNotAttemptToCreateActivationsOnBusySilos",
    async () => {
      // a CPU usage of 100% will disqualify a silo from getting new grains.
      await elasticityGrainPlacementTest(
        (g) => g.latchCpuUsage(100.0),
        (g) => g.unlatchCpuUsage(),
      );
    },
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_CatchingUp",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_StoppingSilos",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_AllSilosCPUTooHigh",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_AllSilosOverloaded",
  );
});
