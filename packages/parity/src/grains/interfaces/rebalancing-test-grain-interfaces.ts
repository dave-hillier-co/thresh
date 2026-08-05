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
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Guid } from "@thresh/core/guid";

export interface IRebalancingTestGrain extends GrainKey<Guid> {
  ping(): Promise<void>;
}

export const IRebalancingTestGrain = defineGrainInterface<IRebalancingTestGrain>(
  "UnitTests.ActivationRebalancingTests.IRebalancingTestGrain",
);
