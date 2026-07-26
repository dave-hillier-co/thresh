// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/GrainCancellationTokenTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { Guid } from "@thresh/core/guid";
import type { ClientNode } from "@thresh/client/client-node";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";
import {
  ILongRunningTaskGrain,
  LongRunningTaskGrain,
} from "@thresh/parity/grains/impl/long-running-task-grain";
import { randomGuidKey } from "@thresh/parity/support/keys";
import {
  cancelUntilSettled,
  clientTwoGrains,
  flushPerDelay,
  twoGrains,
} from "@thresh/parity/support/cancellation";
import { createClusterClient } from "@thresh/parity/support/client";

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
  let client: ClientNode;

  // Upstream runs against a cluster with at least one secondary silo (for
  // InterSiloGrainCancellation); TestCluster defaults to 2.
  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [{ ctor: LongRunningTaskGrain, interfaces: [ILongRunningTaskGrain] }],
    });
    client = await createClusterClient(cluster, [
      { ctor: LongRunningTaskGrain, interfaces: [ILongRunningTaskGrain] },
    ]);
  });

  afterAll(async () => {
    await client.close();
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

  // A `GrainCancellationToken.register` callback that throws when the token is
  // cancelled propagates its exception back through `cts.cancel()` (Orleans:
  // the callback's exception flows to `GrainCancellationTokenSource.Cancel()`).
  // The grain call itself is fire-and-forget (upstream `.Ignore()`); its own
  // cooperative-cancellation rejection is caught so it does not surface as an
  // unhandled rejection. Asserting on the message rather than the error class
  // keeps this indifferent to whether the grain activated on a peer silo (a
  // rejection crossing the wire degrades to a generic error carrying the same
  // message — see `expectCancelled`).
  orleansTest(
    `${testClass}.CancellationTokenCallbacksThrow_ExceptionShouldBePropagated`,
    async () => {
      const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
      const cts = cluster.newCancellationTokenSource();
      const callId = Guid.newGuid().toString();
      const grainTask = grain.grainCancellationTokenCallbackThrow(cts.token, callId);
      grainTask.catch(() => {});
      // Ensure the callback is registered before cancelling (deterministic
      // stand-in for upstream's `await Task.Delay(100)`).
      await waitFor(() => grain.wasStarted(callId));
      await expect(cts.cancel()).rejects.toThrow(/From cancellation token callback/i);
      await waitFor(() => grain.wasCancelled(callId));
    },
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

  // Upstream forwards the client's token through a SECOND grain hop
  // (`grain.CallOtherLongRunningTaskGrainCancellation(target, cts.Token, ...)`
  // forwards to `target`), and asserts `target` observes the cancellation.
  // `ClientNode.newCancellationTokenSource()` makes a direct client -> grain
  // call cancellable (`recordTarget` runs on the client's own outgoing
  // dispatch, before that call's `invoke()` ever serializes it). The client
  // call always crosses the wire, so the first-hop grain (`grain`) receives a
  // token rebuilt from a wire `CancellationTokenPlaceholder` with no `source`
  // — but the activation that binds it now also binds an
  // `onDispatchToTarget` hook (`ActivationData.bindCancellationTokens`), so
  // when `grain` forwards that token on to `target`, the forward is recorded
  // on `grain`'s own `CancellationSourcesExtension` instead
  // (`recordCancellationTarget`). The client's `cts.cancel()` reaches
  // `grain` (a direct, recorded target of the client's source) and `grain`'s
  // `cancelRemoteToken` cascades on to `target` (its own recorded forward),
  // so `target` observes the cancellation without the client's source ever
  // learning about it directly.
  orleansTest.each([0, 10, 300])(
    `${testClass}.InSiloClientCancellationTokenPassing`,
    async (delay) => {
      const { a: grain, b: target } = await clientTwoGrains(cluster, client, true);
      const cts = client.newCancellationTokenSource();
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
      await waitFor(() => target.wasCancelled(callId));
    },
  );

  orleansTest.each([0, 10, 300])(
    `${testClass}.InterSiloClientCancellationTokenPassing`,
    async (delay) => {
      const { a: grain, b: target } = await clientTwoGrains(cluster, client, false);
      const cts = client.newCancellationTokenSource();
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
      await waitFor(() => target.wasCancelled(callId));
    },
  );
});
