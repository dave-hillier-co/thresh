// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/RebalancingTestBase.cs @ v10.1.0 (MIT).
//
// Upstream declares `IRebalancingTestGrain`/`RebalancingTestGrain` inline in
// the test base class rather than in a shared grain-interfaces file — ported
// here as a standalone grain so it can be registered on a `TestCluster` like
// any other parity grain. Default (random) placement, same as upstream's
// plain `Grain` with no placement attribute — the rebalancing tests seed a
// skewed distribution across silos entirely via the
// `IPlacementDirector.PlacementHintKey` `RequestContext` hint, not via a
// dedicated placement strategy.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";

export interface IRebalancingTestGrain extends GrainWithGuidKey {
  ping(): Promise<void>;
}

export const IRebalancingTestGrain = defineGrainInterface<IRebalancingTestGrain>(
  "UnitTests.ActivationRebalancingTests.IRebalancingTestGrain",
);
