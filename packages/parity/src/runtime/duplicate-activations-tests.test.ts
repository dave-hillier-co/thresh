// Ported from dotnet/orleans test/Orleans.Runtime.Tests/DuplicateActivationsTests.cs @ v10.1.0 (MIT).
// Upstream also bumps SiloMessagingOptions.ResponseTimeout to 1 minute to
// accommodate stress-test load; this framework applies no per-call response
// timeout by default (one is opt-in via `defaultResponseTimeout`), so there is
// nothing to configure here.
import { afterAll, beforeAll, describe } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { CatalogTestGrain, ICatalogTestGrain } from "@thresh/parity/grains/impl/catalog-test-grain";

describe("UnitTests.CatalogTests.DuplicateActivationsTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [{ ctor: CatalogTestGrain, interfaces: [ICatalogTestGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.CatalogTests.DuplicateActivationsTests.DuplicateActivations",
    async () => {
      const nRunnerGrains = 100; // Number of grains making concurrent calls
      const nTargetGrain = 10; // Number of target grains (high contention)
      const startingKey = 1000n; // Starting grain ID for target grains
      const nCallsToEach = 100; // Calls each runner makes to each target

      // Phase 1: Initialize all runner grains.
      // Using negative IDs for runners to avoid collision with target grain IDs.
      const runnerGrains = Array.from({ length: nRunnerGrains }, (_, i) =>
        cluster.getGrain(ICatalogTestGrain, BigInt(-i)),
      );
      await Promise.all(runnerGrains.map((g) => g.initialize()));

      // Phase 2: All runners simultaneously blast calls to the same target grains.
      // This creates massive concurrent activation pressure on the catalog.
      // If any duplicate activations occur, the grains will detect and report them.
      await Promise.all(
        runnerGrains.map((g) => g.blastCallNewGrains(nTargetGrain, startingKey, nCallsToEach)),
      );
    },
    // Stress test: 100k in-process calls take ~30s alone, longer when vitest
    // runs files in parallel — hence a budget well above the project default.
    180_000,
  );
});
