// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/GrainCancellationTokenTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainCancellationTokenSource } from "@tsva/core/grain-cancellation-token";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import { Guid } from "@tsva/core/guid";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import {
  ILongRunningTaskGrain,
  LongRunningTaskGrain,
} from "@tsva/parity/grains/impl/long-running-task-grain";
import { randomGuidKey } from "@tsva/parity/support/keys";

const testClass = "UnitTests.CancellationTests.GrainCancellationTokenTests";
const grainType = getGrainMetadata(LongRunningTaskGrain)!.grainType;

/** Every rejection this suite checks for is a cooperative cancellation, whether
 * it arrives as the exact `GrainTaskCanceledError` (in-process) or a generic
 * `GrainCallError` carrying its message (crossed a silo boundary — only
 * `RejectionError` subtypes survive `ClusterNode.serializeError`; see
 * `cancellation.cluster.test.ts`). Asserting on the message rather than the
 * instance keeps this suite indifferent to which silo a grain happened to
 * activate on. */
async function expectCancelled(task: Promise<unknown>): Promise<void> {
  await expect(task).rejects.toThrow(/cancelled/i);
}

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
async function cancelUntilSettled(
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
async function flushPerDelay(delay: number): Promise<void> {
  if (delay > 0) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
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
async function twoGrains(
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

describe(testClass, () => {
  let cluster: TestCluster;

  // Upstream runs against a cluster with at least one secondary silo (for
  // InterSiloGrainCancellation); TestCluster defaults to 2.
  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [{ ctor: LongRunningTaskGrain, interfaces: [ILongRunningTaskGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest.each([0, 10, 300])(`${testClass}.GrainTaskCancellation`, async (delay) => {
    const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
    const cts = cluster.newCancellationTokenSource();
    const callId = Guid.newGuid().toString();
    const grainTask = grain.longWaitGrainCancellation(cts.token, 10_000, callId);
    await flushPerDelay(delay);
    await cancelUntilSettled([{ cts, task: grainTask }]);
    await expectCancelled(grainTask);
    if (delay > 0) {
      await waitFor(() => grain.wasCancelled(callId));
    }
  });

  orleansTest.each([0, 10, 300])(`${testClass}.MultipleGrainsTaskCancellation`, async (delay) => {
    const cts = cluster.newCancellationTokenSource();
    const callId = Guid.newGuid().toString();
    const grains = Array.from({ length: 5 }, () =>
      cluster.getGrain(ILongRunningTaskGrain, randomGuidKey()),
    );
    const grainTasks = grains.map((grain) =>
      grain.longWaitGrainCancellationInterleaving(cts.token, 10_000, callId),
    );
    await flushPerDelay(delay);
    await cancelUntilSettled(grainTasks.map((task) => ({ cts, task })));
    await Promise.all(grainTasks.map((task) => expectCancelled(task)));
    if (delay > 0) {
      for (const grain of grains) {
        await waitFor(() => grain.wasCancelled(callId));
      }
    }
  });

  orleansTest.each([0, 10, 300])(`${testClass}.GrainTaskMultipleCancellations`, async (delay) => {
    const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
    const callIds = Array.from({ length: 5 }, () => Guid.newGuid().toString());
    const sources = callIds.map(() => cluster.newCancellationTokenSource());
    const tasks = callIds.map((callId, index) =>
      grain.longWaitGrainCancellationInterleaving(sources[index]!.token, 10_000, callId),
    );
    await flushPerDelay(delay);
    await cancelUntilSettled(sources.map((cts, index) => ({ cts, task: tasks[index]! })));
    await Promise.all(tasks.map((task) => expectCancelled(task)));
    if (delay > 0) {
      for (const callId of callIds) {
        await waitFor(() => grain.wasCancelled(callId));
      }
    }
  });

  orleansTest(
    `${testClass}.TokenPassingWithoutCancellation_NoExceptionShouldBeThrown`,
    async () => {
      const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
      const cts = cluster.newCancellationTokenSource();
      // Upstream: TimeSpan.FromMilliseconds(1). The grain's delay is a raw
      // `setTimeout` (not clock-driven), so a small real delay is unavoidable
      // here; it is never cancelled, so this just proves the token passes
      // through cleanly.
      await grain.longWaitGrainCancellation(cts.token, 10, "00000000-0000-0000-0000-000000000000");
    },
  );

  orleansTest(`${testClass}.PreCancelledTokenPassing`, async () => {
    const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
    const cts = cluster.newCancellationTokenSource();
    await cts.cancel();
    const callId = Guid.newGuid().toString();
    const grainTask = grain.longWaitGrainCancellation(cts.token, 10_000, callId);
    await expectCancelled(grainTask);
    await waitFor(() => grain.wasCancelled(callId));
  });

  orleansTest.excluded(
    "exercises .NET's ExecutionContext flowing through a CancellationToken callback registered inside the grain method; JS has no ExecutionContext/TaskScheduler equivalent to assert on.",
    `${testClass}.CancellationTokenCallbacksExecutionContext`,
  );
  orleansTest.excluded(
    "exercises which TaskScheduler a CancellationToken callback runs on across a grain-to-grain call; no TaskScheduler equivalent exists in this runtime.",
    `${testClass}.CancellationTokenCallbacksTaskSchedulerContext`,
  );

  // Needs token-callback registration (Orleans `GrainCancellationToken
  // .CancellationToken.Register(...)`, i.e. a callback fired when the token
  // is cancelled, whose thrown exception propagates back out of `cancel()`).
  // `GrainCancellationToken` here exposes only `signal`/`isCancellationRequested`
  // /`throwIfCancellationRequested` — no callback-registration API — so this
  // is a genuine feature gap, not a .NET-only mechanism.
  orleansTest.gap(
    "GAP-CANCELLATION",
    `${testClass}.CancellationTokenCallbacksThrow_ExceptionShouldBePropagated`,
  );

  orleansTest.each([0, 10, 300])(`${testClass}.InSiloGrainCancellation`, async (delay) => {
    const { a: grain, b: target } = await twoGrains(cluster, true);
    const cts = cluster.newCancellationTokenSource();
    const callId = Guid.newGuid().toString();
    const grainTask = grain.callOtherLongRunningTaskGrainCancellation(
      target,
      cts.token,
      10_000,
      callId,
    );
    await flushPerDelay(delay);
    await cancelUntilSettled([{ cts, task: grainTask }]);
    await expectCancelled(grainTask);
    if (delay > 0) {
      await waitFor(() => target.wasCancelled(callId));
    }
  });

  orleansTest.each([0, 10, 300])(`${testClass}.InterSiloGrainCancellation`, async (delay) => {
    const { a: grain, b: target } = await twoGrains(cluster, false);
    const cts = cluster.newCancellationTokenSource();
    const callId = Guid.newGuid().toString();
    const grainTask = grain.callOtherLongRunningTaskGrainCancellation(
      target,
      cts.token,
      10_000,
      callId,
    );
    await flushPerDelay(delay);
    await cancelUntilSettled([{ cts, task: grainTask }]);
    await expectCancelled(grainTask);
    if (delay > 0) {
      await waitFor(() => target.wasCancelled(callId));
    }
  });

  // Needs a client process hosting the `GrainCancellationTokenSource` itself
  // (Orleans: the test's own client is the source, not a grain) — this
  // harness's `TestCluster` has no separate client distinct from a silo
  // (GAP-CLIENT-SILO-SEPARATION); the grain-driven cancellation core is
  // proven above (InSilo/InterSiloGrainCancellation, GrainTaskCancellation).
  orleansTest.gap(
    "GAP-CANCELLATION",
    `${testClass}.InterSiloClientCancellationTokenPassing`,
  );
  orleansTest.gap(
    "GAP-CANCELLATION",
    `${testClass}.InSiloClientCancellationTokenPassing`,
  );
});
