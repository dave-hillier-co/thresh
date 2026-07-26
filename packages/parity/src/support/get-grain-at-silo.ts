import { PLACEMENT_HINT_KEY, RequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { TestCluster } from "@thresh/testing/test-cluster";
import { IRandomPlacementTestGrain } from "@thresh/parity/grains/impl/placement-test-grain";
import { randomGuidKey } from "@thresh/parity/support/keys";

/**
 * Upstream `ElasticPlacementTests.GetGrainAtSilo` (and other ported suites'
 * identically-shaped private helpers): pin a fresh `IRandomPlacementTestGrain`
 * onto `silo` via the `RequestContext`-driven placement hint
 * (`IPlacementDirector.PlacementHintKey`), retrying with a new key until
 * placement actually lands there — the hint is only honoured when the target
 * is a live candidate, so a stale/dead `silo` would spin forever, same as
 * upstream.
 *
 * `RequestContext` is ambient here (an `AsyncLocalStorage`, see
 * `@thresh/core/request-context`): unlike upstream, where the hint rides one
 * captured-at-send-time message header per call, `RequestContext.set` stays
 * in scope for every subsequent call this same async chain makes. So the
 * hint is removed again once a match is found — a caller that pins a grain
 * here and goes on to place OTHER grains right after (e.g.
 * `ElasticPlacementTests.ElasticityGrainPlacementTest`, sampling placement of
 * ten fresh grains) must not have every one of them pinned to `silo` too.
 */
export async function getGrainAtSilo(
  cluster: TestCluster,
  silo: SiloAddress,
): Promise<IRandomPlacementTestGrain> {
  const target = silo.toString();
  for (;;) {
    RequestContext.set(PLACEMENT_HINT_KEY, target);
    const grain = cluster.getGrain(IRandomPlacementTestGrain, randomGuidKey());
    const location = await grain.getRuntimeInstanceId();
    RequestContext.remove(PLACEMENT_HINT_KEY);
    if (location === target) return grain;
  }
}
