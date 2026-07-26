// Shared helpers for the `ActivationRebalancingTests` parity suite, ported
// from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/RebalancingTestBase.cs
// @ v10.1.0 (MIT). Upstream's `RebalancingTestBase<TFixture>` bundles these as
// protected members of a shared base class; this framework has no shared
// fixture/base-class convention for `orleansTest`-style suites, so they are
// plain functions each test file imports instead.
import { Guid } from "@thresh/core/guid";
import { PLACEMENT_HINT_KEY, RequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { DetailedGrainStatistic } from "@thresh/core/management-grain";
import type { TestCluster } from "@thresh/testing/test-cluster";
import { IRebalancingTestGrain } from "@thresh/parity/grains/interfaces/rebalancing-test-grain-interfaces";

/** Live activations of `IRebalancingTestGrain` currently hosted on `silo` (Orleans `GetActivationCount`). */
export function getActivationCount(
  stats: readonly DetailedGrainStatistic[],
  silo: SiloAddress,
): number {
  return stats.filter((s) => s.silo.equals(silo)).length;
}

/**
 * Force `count` fresh `IRebalancingTestGrain` activations onto `silo` via the
 * `PLACEMENT_HINT_KEY` `RequestContext` hint (Orleans
 * `RequestContext.Set(IPlacementDirector.PlacementHintKey, silo)`), pushing
 * each call's promise onto `tasks` rather than awaiting inline — the caller
 * awaits them all together.
 */
export function addTestActivations(
  cluster: TestCluster,
  tasks: Promise<unknown>[],
  silo: SiloAddress,
  count: number,
): void {
  RequestContext.set(PLACEMENT_HINT_KEY, silo.toString());
  for (let i = 0; i < count; i += 1) {
    tasks.push(cluster.getGrain(IRebalancingTestGrain, Guid.newGuid()).ping());
  }
}

/** Drain microtasks repeatedly so cross-silo async migration settles between rebalancer ticks. */
const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
export async function settle(rounds = 8): Promise<void> {
  for (let i = 0; i < rounds; i += 1) await flush();
}
