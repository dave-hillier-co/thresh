// Shared helpers for the cancellation-token test suites: both
// `grain-cancellation-token-tests.test.ts` (Orleans `GrainCancellationTokenTests`)
// and `cancellation-token-tests.test.ts` (Orleans `CancellationTokenTests`) drive
// the same `LongRunningTaskGrain` fixture through the same cancel/observe idioms.
import type { GrainCancellationTokenSource } from "@tsva/core/grain-cancellation-token";
import { GrainId } from "@tsva/core/grain-id";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import type { ClientNode } from "@tsva/client/client-node";
import type { TestCluster, TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import {
  ILongRunningTaskGrain,
  LongRunningTaskGrain,
} from "@tsva/parity/grains/impl/long-running-task-grain";
import { randomGuidKey } from "@tsva/parity/support/keys";

export const grainType = getGrainMetadata(LongRunningTaskGrain)!.grainType;

/**
 * Cancel every `{ cts, task }` pair's source, retrying (`cts.cancel()` is
 * idempotent) until every task has settled or `waitFor`'s deadline elapses.
 * A single `cancel()` only reaches targets its source has recorded so far;
 * for a grain-to-grain call (`InSilo`/`InterSiloGrainCancellation`) the
 * *second* hop's target is recorded only once the first callee's own
 * outgoing call has been dispatched, which can lag a single `cancel()` when
 * it crosses another silo. Retrying (rather than guessing a fixed wait)
 * catches up regardless of how many hops or which silos are involved.
 */
export async function cancelUntilSettled(
  pairs: ReadonlyArray<{ cts: GrainCancellationTokenSource; task: Promise<unknown> }>,
): Promise<void> {
  const pending = new Set(pairs.map((_, index) => index));
  for (const [index, pair] of pairs.entries()) {
    pair.task.then(
      () => pending.delete(index),
      () => pending.delete(index),
    );
  }
  await waitFor(async () => {
    if (pending.size === 0) return true;
    await Promise.all(pairs.map((pair) => pair.cts.cancel()));
    return pending.size === 0;
  });
}

/**
 * Deterministic stand-in for upstream's `await Task.Delay(delay)` before
 * cancelling: `delay === 0` cancels with no yield at all (racing the call
 * itself, exactly like `PreCancelledTokenPassing` when the race goes the
 * other way); `delay > 0` yields one macrotask tick first, so the callee's
 * turn has been admitted and started its cancellable await before the first
 * cancel attempt. Either way, `cancelUntilSettled` (not this) is what
 * actually guarantees cancellation lands — this only shapes *when the first
 * attempt* happens, matching upstream's "cancel immediately" vs "cancel
 * after some delay" distinction. Never waits out real seconds — the grain's
 * own 10s delay never elapses either way.
 */
export async function flushPerDelay(delay: number): Promise<void> {
  if (delay > 0) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

export function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
  return cluster.silos.find((s) => s.host.isActive(grainId));
}

/**
 * Upstream's `GetGrains`: retry fresh keys until the two grains land on the
 * same silo (`sameSilo: true`) or different silos (`false`) — the closest
 * available stand-in for `RequestContext.Set(PlacementHintKey, ...)`, which
 * has no equivalent here (no placement-hint mechanism exists). Mirrors
 * `grain-placement-cluster-change-tests.test.ts`'s retry-loop idiom.
 *
 * For the `sameSilo: false` (inter-silo) case, `a` is additionally pinned to
 * `cluster.primary`: `cluster.getGrain` always calls through the primary
 * silo's factory, so a test -> `a` call that lands on a *different* silo
 * itself crosses the wire, which strips the caller-side
 * `GrainCancellationTokenSource` back-reference a wire-arrived token carries
 * (see `cancellationTokenSourceOf`) — without it, `a`'s own forwarding call
 * to `b` can never record `b` as a target on the test's source, so
 * cancellation would have no way to reach `b` at all (not merely slowly —
 * never). Pinning `a` to primary keeps the test -> `a` hop in-process, so
 * only the interesting `a` -> `b` hop actually crosses silos.
 */
export async function twoGrains(
  cluster: TestCluster,
  sameSilo: boolean,
): Promise<{ a: ILongRunningTaskGrain; b: ILongRunningTaskGrain }> {
  for (;;) {
    const aKey = randomGuidKey();
    const bKey = randomGuidKey();
    const a = cluster.getGrain(ILongRunningTaskGrain, aKey);
    const b = cluster.getGrain(ILongRunningTaskGrain, bKey);
    // Force activation so `hostOf` has something to find.
    await a.wasCancelled("warmup");
    await b.wasCancelled("warmup");
    const hostA = hostOf(cluster, new GrainId(grainType, aKey));
    const hostB = hostOf(cluster, new GrainId(grainType, bKey));
    if (hostA === undefined || hostB === undefined) continue;
    if ((hostA === hostB) !== sameSilo) continue;
    if (!sameSilo && hostA !== cluster.primary) continue;
    return { a, b };
  }
}

/**
 * `twoGrains`'s client-driven counterpart. `a` is reached via `client.getGrain`
 * so the client -> `a` hop (the first of the two forwarded hops under test) is
 * actually exercised, wire and all; a client-originated call always activates
 * on its gateway silo (`ClusterNode.receiveRequest` delivers locally with no
 * cross-silo placement choice — unlike an ordinary `invoke()`, which does
 * consult the directory/placement strategy), so `a` deterministically lands on
 * `cluster.primary` every time, no retry needed. `b` therefore must be warmed
 * up through an ORDINARY (non-client) call — `cluster.getGrain`'s call runs
 * real placement — retried with fresh keys until it lands where `sameSilo`
 * wants relative to `a`'s fixed gateway host; warming `b` up through the
 * client too would activate it on the very same gateway silo as `a` every
 * time, making an inter-silo split unreachable. Cascading cancellation
 * (`CancellationSourcesExtension` forwarding, see `cancellation-extension.ts`)
 * is what lets the client's `cts.cancel()` reach `b` regardless of how many
 * silo boundaries the client -> `a` -> `b` chain crosses.
 */
export async function clientTwoGrains(
  cluster: TestCluster,
  client: ClientNode,
  sameSilo: boolean,
): Promise<{ a: ILongRunningTaskGrain; b: ILongRunningTaskGrain }> {
  const aKey = randomGuidKey();
  const a = client.getGrain(ILongRunningTaskGrain, aKey);
  await a.wasCancelled("warmup");
  const hostA = hostOf(cluster, new GrainId(grainType, aKey));
  for (;;) {
    const bKey = randomGuidKey();
    await cluster.getGrain(ILongRunningTaskGrain, bKey).wasCancelled("warmup");
    const hostB = hostOf(cluster, new GrainId(grainType, bKey));
    if (hostA === undefined || hostB === undefined) continue;
    if ((hostA === hostB) !== sameSilo) continue;
    return { a, b: client.getGrain(ILongRunningTaskGrain, bKey) };
  }
}
