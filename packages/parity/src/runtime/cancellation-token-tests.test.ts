// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/CancellationTokenTests.cs @ v10.1.0 (MIT).
//
// Upstream is an abstract base class run twice via two IClassFixture subclasses
// (CancellationTokenTests_WaitForAcknowledgement / _NoWaitForAcknowledgement),
// distinguished only by `SiloMessagingOptions.WaitForCancellationAcknowledgement`
// (true / false respectively) — whether the caller's cancel blocks until the
// callee has acknowledged. JS has a single cancellation-token type
// (`GrainCancellationToken` wrapping `AbortSignal`; see
// `grain-cancellation-token-tests.test.ts`), and this runtime's `cancel()`
// always awaits the callee's cancellation acknowledgement — equivalent to
// `WaitForCancellationAcknowledgement = true` unconditionally. None of the
// ported assertions below observe ack-timing (they only assert that
// cancellation eventually propagates), so this satisfies every assertion in
// both fixtures; the "no wait" option is not modelled since no ported test
// distinguishes it, so both fixtures below exercise identical behaviour.
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

/** Same cross-silo caveat as the sibling suite: assert on the rejection
 * message, not the error class/instance, since a rejection crossing a silo
 * boundary degrades a `GrainTaskCanceledError` to a generic `GrainCallError`
 * carrying the same message — see
 * `grain-cancellation-token-tests.test.ts`'s `expectCancelled`. */
async function expectCancelled(task: Promise<unknown>): Promise<void> {
  await expect(task).rejects.toThrow(/cancel/i);
}

describe("UnitTests.CancellationTests.CancellationTokenTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  // Upstream runs against a cluster with at least one secondary silo (for
  // InterSiloCancellation); TestCluster defaults to 2.
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

  for (const testClass of [
    "UnitTests.CancellationTests.CancellationTokenTests_WaitForAcknowledgement",
    "UnitTests.CancellationTests.CancellationTokenTests_NoWaitForAcknowledgement",
  ]) {
    orleansTest.each([0, 10, 300])(`${testClass}.GrainTaskCancellation`, async (delay) => {
      const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
      const cts = cluster.newCancellationTokenSource();
      const callId = Guid.newGuid().toString();
      const grainTask = grain.longWait(cts.token, 10_000, callId);
      await flushPerDelay(delay);
      await cancelUntilSettled([{ cts, task: grainTask }]);
      await expectCancelled(grainTask);
      if (delay > 0) {
        await waitFor(() => grain.wasCancelled(callId));
      }
    });

    orleansTest.each([0, 10, 300])(
      `${testClass}.MultipleGrainsTaskCancellation`,
      async (delay) => {
        const cts = cluster.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        const grains = Array.from({ length: 5 }, () =>
          cluster.getGrain(ILongRunningTaskGrain, randomGuidKey()),
        );
        const grainTasks = grains.map((grain) =>
          grain.longWaitInterleaving(cts.token, 10_000, callId),
        );
        await flushPerDelay(delay);
        await cancelUntilSettled(grainTasks.map((task) => ({ cts, task })));
        await Promise.all(grainTasks.map((task) => expectCancelled(task)));
        if (delay > 0) {
          for (const grain of grains) {
            await waitFor(() => grain.wasCancelled(callId));
          }
        }
      },
    );

    orleansTest.each([0, 10, 300])(
      `${testClass}.GrainTaskMultipleCancellations`,
      async (delay) => {
        const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
        const callIds = Array.from({ length: 5 }, () => Guid.newGuid().toString());
        const sources = callIds.map(() => cluster.newCancellationTokenSource());
        const tasks = callIds.map((callId, index) =>
          grain.longWaitInterleaving(sources[index]!.token, 10_000, callId),
        );
        await flushPerDelay(delay);
        await cancelUntilSettled(sources.map((cts, index) => ({ cts, task: tasks[index]! })));
        await Promise.all(tasks.map((task) => expectCancelled(task)));
        if (delay > 0) {
          for (const callId of callIds) {
            await waitFor(() => grain.wasCancelled(callId));
          }
        }
      },
    );

    orleansTest(
      `${testClass}.TokenPassingWithoutCancellation_NoExceptionShouldBeThrown`,
      async () => {
        const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
        const cts = cluster.newCancellationTokenSource();
        // Upstream: TimeSpan.FromMilliseconds(1). The grain's delay is a raw
        // `setTimeout` (not clock-driven), so a small real delay is
        // unavoidable here; it is never cancelled, so this just proves the
        // token passes through cleanly.
        await grain.longWait(cts.token, 10, "00000000-0000-0000-0000-000000000000");
      },
    );

    orleansTest(`${testClass}.PreCancelledTokenPassing`, async () => {
      const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
      const cts = cluster.newCancellationTokenSource();
      await cts.cancel();
      const callId = Guid.newGuid().toString();
      const grainTask = grain.longWait(cts.token, 10_000, callId);
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

    // A cancellation callback that throws when fired is cooperative: unlike a
    // `GrainCancellationToken` callback (sibling `GrainCancellationTokenTests`
    // suite), a plain-token callback's exception is contained and does NOT
    // propagate to the canceller — `cancelUntilSettled` drives `cts.cancel()`
    // to completion without observing any failure. The call is still
    // observably cancelled (its `callId` is recorded). The grain call is
    // fire-and-forget (upstream `.Ignore()`); its own cooperative-cancellation
    // rejection is passed to `cancelUntilSettled` (and pre-caught) so it never
    // surfaces as an unhandled rejection.
    orleansTest(
      `${testClass}.CancellationTokenCallbacksThrow_ExceptionDoesNotPropagate`,
      async () => {
        const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
        const cts = cluster.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        const grainTask = grain.cancellationTokenCallbackThrow(cts.token, callId);
        grainTask.catch(() => {});
        await waitFor(() => grain.wasStarted(callId));
        await cancelUntilSettled([{ cts, task: grainTask }]);
        await waitFor(() => grain.wasCancelled(callId));
      },
    );

    orleansTest.each([0, 10, 300])(`${testClass}.InSiloCancellation`, async (delay) => {
      const { a: grain, b: target } = await twoGrains(cluster, true);
      const cts = cluster.newCancellationTokenSource();
      const callId = Guid.newGuid().toString();
      const grainTask = grain.callOtherLongRunningTask(target, cts.token, 10_000, callId);
      await flushPerDelay(delay);
      await cancelUntilSettled([{ cts, task: grainTask }]);
      await expectCancelled(grainTask);
      if (delay > 0) {
        await waitFor(() => target.wasCancelled(callId));
      }
    });

    orleansTest.each([0, 10, 300])(`${testClass}.InterSiloCancellation`, async (delay) => {
      const { a: grain, b: target } = await twoGrains(cluster, false);
      const cts = cluster.newCancellationTokenSource();
      const callId = Guid.newGuid().toString();
      const grainTask = grain.callOtherLongRunningTask(target, cts.token, 10_000, callId);
      await flushPerDelay(delay);
      await cancelUntilSettled([{ cts, task: grainTask }]);
      await expectCancelled(grainTask);
      if (delay > 0) {
        await waitFor(() => target.wasCancelled(callId));
      }
    });

    // Upstream forwards the client's token through a SECOND grain hop
    // (`grain.CallOtherLongRunningTask(target, cts.Token, ...)` forwards to
    // `target`), and asserts `target` observes the cancellation.
    // `ClientNode.newCancellationTokenSource()` makes a direct client -> grain
    // call cancellable; the client call always crosses the wire, so the
    // first-hop grain's token is rebuilt from a wire
    // `CancellationTokenPlaceholder` with no `source` — but the activation
    // that binds it also binds an `onDispatchToTarget` hook
    // (`ActivationData.bindCancellationTokens`), so when the first-hop grain
    // forwards the token on to `target`, the forward is recorded on that
    // activation's own `CancellationSourcesExtension` instead
    // (`recordCancellationTarget`). The client's `cts.cancel()` reaches the
    // first-hop grain (a direct, recorded target of the client's source),
    // and that grain's `cancelRemoteToken` cascades on to `target` (its own
    // recorded forward), so `target` observes the cancellation.
    orleansTest.each([0, 10, 300])(
      `${testClass}.InSiloClientCancellationTokenPassing`,
      async (delay) => {
        const { a: grain, b: target } = await clientTwoGrains(cluster, client, true);
        const cts = client.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        const grainTask = grain.callOtherLongRunningTask(target, cts.token, 10_000, callId);
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
        const grainTask = grain.callOtherLongRunningTask(target, cts.token, 10_000, callId);
        await flushPerDelay(delay);
        await cancelUntilSettled([{ cts, task: grainTask }]);
        await expectCancelled(grainTask);
        await waitFor(() => target.wasCancelled(callId));
      },
    );

    orleansTest.each([0, 10, 300])(
      `${testClass}.InterleavingGrainTaskCancellation`,
      async (delay) => {
        const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());
        const cts = cluster.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        const grainTask = grain.longWaitInterleaving(cts.token, 10_000, callId);
        await flushPerDelay(delay);
        await cancelUntilSettled([{ cts, task: grainTask }]);
        await expectCancelled(grainTask);
        if (delay > 0) {
          await waitFor(() => grain.wasCancelled(callId));
        }
      },
    );

    orleansTest(`${testClass}.CancelInterleavingWhileRegularGrainRequestRunning`, async () => {
      const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());

      // Start a regular (non-interleaving) long-running request. Regular
      // calls occupy the grain's turn, so nothing else on this grain runs
      // concurrently with it unless it is itself marked alwaysInterleave.
      const regularCts = cluster.newCancellationTokenSource();
      const regularCallId = Guid.newGuid().toString();
      const regularTask = grain.longWait(regularCts.token, 30_000, regularCallId);

      // Deterministic stand-in for upstream's `await Task.Delay(100)`: yield
      // one macrotask tick so the regular call is admitted and has started
      // its cancellable delay before anything else is scheduled against the
      // grain. Never waits out real seconds — the grain's own 30s/10s delays
      // never elapse.
      await flushPerDelay(1);

      // Start an interleaving request concurrently: `[AlwaysInterleave]`
      // lets it run even though the regular call still occupies the grain's
      // turn.
      const interleavingCts = cluster.newCancellationTokenSource();
      const interleavingCallId = Guid.newGuid().toString();
      const interleavingTask = grain.longWaitInterleaving(
        interleavingCts.token,
        10_000,
        interleavingCallId,
      );
      await flushPerDelay(1);

      // Cancel only the interleaving call. It must reject, proving
      // cancellation reaches an interleaving call even while a regular call
      // still occupies the grain's turn — and its cancellation is observable
      // (via the interleaving `wasCancelled` query) while the 30s regular call
      // is still pending, matching upstream's WaitForCallCancellation here.
      await cancelUntilSettled([{ cts: interleavingCts, task: interleavingTask }]);
      await expectCancelled(interleavingTask);
      await waitFor(() => grain.wasCancelled(interleavingCallId));

      // Clean up: cancel the regular request too, so nothing is left hanging.
      await cancelUntilSettled([{ cts: regularCts, task: regularTask }]);
      await expectCancelled(regularTask);
    });

    orleansTest(`${testClass}.MultipleInterleavingGrainRequestsCancellation`, async () => {
      const grain = cluster.getGrain(ILongRunningTaskGrain, randomGuidKey());

      const cts1 = cluster.newCancellationTokenSource();
      const cts2 = cluster.newCancellationTokenSource();
      const cts3 = cluster.newCancellationTokenSource();
      const callId1 = Guid.newGuid().toString();
      const callId2 = Guid.newGuid().toString();
      const callId3 = Guid.newGuid().toString();

      const task1 = grain.longWaitInterleaving(cts1.token, 10_000, callId1);
      const task2 = grain.longWaitInterleaving(cts2.token, 10_000, callId2);
      const task3 = grain.longWaitInterleaving(cts3.token, 10_000, callId3);

      // Deterministic stand-in for upstream's `await Task.Delay(100)`: yield
      // a tick so all three interleaving calls are admitted and running.
      await flushPerDelay(1);

      // Cancel only the second request.
      await cancelUntilSettled([{ cts: cts2, task: task2 }]);
      await expectCancelled(task2);

      // First and third should still be running; cancel them too.
      await cancelUntilSettled([
        { cts: cts1, task: task1 },
        { cts: cts3, task: task3 },
      ]);
      await expectCancelled(task1);
      await expectCancelled(task3);

      // `wasCancelled` is a regular (non-interleaving) method: querying it
      // right after cancelling only #2, while #1/#3 were still running,
      // would queue behind them and deadlock (it wouldn't be admitted until
      // those alwaysInterleave calls finished on their own). Only check
      // once all three have actually settled and the grain is idle.
      await waitFor(() => grain.wasCancelled(callId2));
      await waitFor(() => grain.wasCancelled(callId1));
      await waitFor(() => grain.wasCancelled(callId3));
    });
  }
});
