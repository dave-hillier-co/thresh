// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/GrainCancellationTokenTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { Guid } from "@tsva/core/guid";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import {
  ILongRunningTaskGrain,
  LongRunningTaskGrain,
} from "@tsva/parity/grains/impl/long-running-task-grain";
import { randomGuidKey } from "@tsva/parity/support/keys";
import { cancelUntilSettled, flushPerDelay, twoGrains } from "@tsva/parity/support/cancellation";

const testClass = "UnitTests.CancellationTests.GrainCancellationTokenTests";

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
