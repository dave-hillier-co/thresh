// Ported from dotnet/orleans test/Orleans.Runtime.Tests/HeterogeneousSilosTests/HeterogeneousTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.HeterogeneousSilosTests.HeterogeneousTests", () => {
  // The whole file depends on DI/hosting-builder machinery this harness does
  // not expose: an `IHostConfigurator` that removes specific grain classes
  // from `GrainTypeOptions` per silo name (a runtime per-silo grain
  // blocklist), a client `TypeManagementOptions.TypeMapRefreshInterval` to
  // control how fast the client's grain-type map converges after a topology
  // change, and reconnecting the cluster client (`StopClusterClientAsync` /
  // `InitializeClientAsync`) to force a fresh type-map fetch. None of that
  // per-silo grain-class exclusion or client type-map refresh surface exists
  // here — grains are registered programmatically per silo via
  // `TestCluster.start({ grains })`, with no DI container to reach into.
  const reason =
    "DI/hosting builders: relies on an IHostConfigurator removing grain classes from GrainTypeOptions per silo, client TypeManagementOptions refresh, and cluster-client reconnect — no equivalent test-instrumentation surface exists in this harness";

  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.HeterogeneousTests.GrainExcludedTest",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.HeterogeneousTests.MergeGrainResolverTests",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.HeterogeneousTests.MergeGrainResolverWithClientRefreshTests",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.HeterogeneousTests.StatelessWorkerPlacementTests",
  );
  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/9560)",
    "Tester.HeterogeneousSilosTests.HeterogeneousTests.StatelessWorkerPlacementWithClientRefreshTests",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
