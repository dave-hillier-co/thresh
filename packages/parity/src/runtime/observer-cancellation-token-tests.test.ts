// Ported from dotnet/orleans test/Orleans.Runtime.Tests/CancellationTests/ObserverCancellationTokenTests.cs @ v10.1.0 (MIT).
//
// Upstream is an abstract base class run twice via two IClassFixture
// subclasses (ObserverCancellationTokenTests_WaitForAcknowledgement /
// _NoWaitForAcknowledgement), distinguished only by
// `SiloMessagingOptions.WaitForCancellationAcknowledgement` (true / false).
// This runtime's `GrainCancellationTokenSource.cancel()` always awaits every
// recorded target's acknowledgement unconditionally (see
// `cancellation-token-tests.test.ts`'s header for the same reasoning), so
// both fixtures below exercise identical behaviour.
//
// The grain (`IObserverWithCancellationGrain`) is driven directly via
// `cluster.getGrain` (an in-process test -> grain hop, NOT through the
// client): the grain-factory's dispatch hook
// (`cancellationTokenSourceOf(arg)?.recordTarget(target)`, `grain-factory.ts`)
// only records a call's target on the token's SOURCE when the token argument
// still carries that back-reference — which survives only across an
// in-process hop (no serialize/deserialize round trip strips it). Driving the
// grain through the client instead would cross the wire on the very first
// hop and permanently lose the source, leaving `cts.cancel()` with no way to
// discover this grain (or, transitively, the observer it forwards to) as a
// target at all. The SECOND hop — the grain calling
// `this.observer.longWait(...)`, a client-hosted observer reference — always
// crosses the wire (silo -> gateway -> client), which is what exercises the
// new client-side cancellation binding under test here.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { GrainTaskCanceledError } from "@thresh/core/errors";
import type { GrainCancellationToken } from "@thresh/core/grain-cancellation-token";
import { Guid } from "@thresh/core/guid";
import type { ClientNode } from "@thresh/client/client-node";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";
import {
  ILongRunningObserver,
  IObserverWithCancellationGrain,
  ObserverWithCancellationGrain,
} from "@thresh/parity/grains/impl/observer-with-cancellation-grain";
import { randomGuidKey } from "@thresh/parity/support/keys";
import { cancelUntilSettled } from "@thresh/parity/support/cancellation";
import { createClusterClient } from "@thresh/parity/support/client";

/** Same cross-silo caveat noted in `cancellation-token-tests.test.ts`: assert
 * on the rejection message, not the error class/instance — the observer
 * notification crosses the wire (silo -> client), which degrades
 * `GrainTaskCanceledError` to a generic error carrying the same message. */
async function expectCancelled(task: Promise<unknown>): Promise<void> {
  await expect(task).rejects.toThrow(/cancel/i);
}

/** Mirrors `LongRunningTaskGrain`'s cancellable-delay helper (`cancellation.cluster.test.ts`). */
function cancellableDelay(token: GrainCancellationToken, delayMs: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (token.isCancellationRequested) {
      reject(new GrainTaskCanceledError());
      return;
    }
    const timer = setTimeout(resolve, delayMs);
    token.signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new GrainTaskCanceledError());
      },
      { once: true },
    );
  });
}

/**
 * Client-hosted `ILongRunningObserver` (Orleans' `LongRunningObserver`,
 * defined inline in the upstream test file). Tracks, per `callId`, whether
 * the call has started and whether it was cancelled — polled via `waitFor`
 * instead of upstream's `TaskCompletionSource`-backed `WaitForCallToStart`/
 * `WaitForCancellation`.
 */
function makeLongRunningObserver(): ILongRunningObserver & {
  hasStarted(callId: string): boolean;
  wasCancelled(callId: string): boolean;
} {
  const started = new Set<string>();
  const cancelled = new Set<string>();

  async function run(
    delayMs: number,
    callId: string,
    token: GrainCancellationToken,
  ): Promise<void> {
    started.add(callId);
    try {
      await cancellableDelay(token, delayMs);
    } catch (err) {
      cancelled.add(callId);
      throw err;
    }
  }

  return {
    longWait: (delayMs, callId, token) => run(delayMs, callId, token),
    interleavingLongWait: (delayMs, callId, token) => run(delayMs, callId, token),
    hasStarted: (callId) => started.has(callId),
    wasCancelled: (callId) => cancelled.has(callId),
  };
}

describe("UnitTests.CancellationTests.ObserverCancellationTokenTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: ObserverWithCancellationGrain, interfaces: [IObserverWithCancellationGrain] },
      ],
    });
    client = await createClusterClient(cluster, []);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  for (const testClass of [
    "UnitTests.CancellationTests.ObserverCancellationTokenTests_WaitForAcknowledgement",
    "UnitTests.CancellationTests.ObserverCancellationTokenTests_NoWaitForAcknowledgement",
  ]) {
    orleansTest.each([true, false])(
      `${testClass}.ObserverTaskCancellation`,
      async (cancelImmediately) => {
        const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
        const observer = makeLongRunningObserver();
        const ref = client.createObjectReference(ILongRunningObserver, observer);
        await grain.subscribe(ref);

        const cts = cluster.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        const grainTask = grain.notifyLongWait(10_000, callId, cts.token);

        if (!cancelImmediately) await waitFor(() => observer.hasStarted(callId));
        await cancelUntilSettled([{ cts, task: grainTask }]);

        await expectCancelled(grainTask);
        if (!cancelImmediately) await waitFor(() => observer.wasCancelled(callId));
        await waitFor(async () => (await grain.getProcessedCancellations()).includes(callId));

        await grain.unsubscribe(ref);
        client.deleteObjectReference(ref);
      },
    );

    orleansTest(`${testClass}.PreCancelledTokenPassing`, async () => {
      const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
      const observer = makeLongRunningObserver();
      const ref = client.createObjectReference(ILongRunningObserver, observer);
      await grain.subscribe(ref);

      const cts = cluster.newCancellationTokenSource();
      await cts.cancel();

      const callId = Guid.newGuid().toString();
      const grainTask = grain.notifyLongWait(10_000, callId, cts.token);
      await expectCancelled(grainTask);
      await waitFor(async () => (await grain.getProcessedCancellations()).includes(callId));

      await grain.unsubscribe(ref);
      client.deleteObjectReference(ref);
    });

    orleansTest(
      `${testClass}.TokenPassingWithoutCancellation_NoExceptionShouldBeThrown`,
      async () => {
        const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
        const observer = makeLongRunningObserver();
        const ref = client.createObjectReference(ILongRunningObserver, observer);
        await grain.subscribe(ref);

        const cts = cluster.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        // Upstream: TimeSpan.FromMilliseconds(1). The observer's delay is a raw
        // `setTimeout` (not clock-driven), so a small real delay is unavoidable
        // here; it is never cancelled, so this just proves the token passes
        // through cleanly across the silo -> client hop.
        await grain.notifyLongWait(10, callId, cts.token);

        await grain.unsubscribe(ref);
        client.deleteObjectReference(ref);
      },
    );

    orleansTest.excluded(
      "exercises .NET's ExecutionContext flowing through a CancellationToken callback registered inside the observer method; the callback's whole purpose here is to assert it runs on the grain's TaskScheduler/ExecutionContext, of which JS has no equivalent (the observable resolve is only meaningful paired with that context check). GrainCancellationToken.register now exists, but there is nothing context-specific left to assert.",
      `${testClass}.CancellationTokenCallbacksExecutionContext`,
    );

    orleansTest.each([true, false])(
      `${testClass}.MultipleObserversCancellation`,
      async (cancelImmediately) => {
        const cts = cluster.newCancellationTokenSource();
        const entries = await Promise.all(
          Array.from({ length: 5 }, async () => {
            const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
            const observer = makeLongRunningObserver();
            const ref = client.createObjectReference(ILongRunningObserver, observer);
            await grain.subscribe(ref);
            return { grain, observer, ref, callId: Guid.newGuid().toString() };
          }),
        );

        const tasks = entries.map((e) => e.grain.notifyLongWait(10_000, e.callId, cts.token));

        if (!cancelImmediately) {
          await Promise.all(entries.map((e) => waitFor(() => e.observer.hasStarted(e.callId))));
        }
        await cancelUntilSettled(tasks.map((task) => ({ cts, task })));

        await Promise.all(tasks.map((task) => expectCancelled(task)));
        if (!cancelImmediately) {
          await Promise.all(entries.map((e) => waitFor(() => e.observer.wasCancelled(e.callId))));
        }
        await Promise.all(
          entries.map((e) =>
            waitFor(async () => (await e.grain.getProcessedCancellations()).includes(e.callId)),
          ),
        );

        for (const e of entries) {
          await e.grain.unsubscribe(e.ref);
          client.deleteObjectReference(e.ref);
        }
      },
    );

    orleansTest(`${testClass}.CancelWaitingRequest`, async () => {
      const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
      const observer = makeLongRunningObserver();
      const ref = client.createObjectReference(ILongRunningObserver, observer);
      await grain.subscribe(ref);

      // A long-running request occupies the grain's turn (notifyLongWait is
      // not alwaysInterleave).
      const blockingCts = cluster.newCancellationTokenSource();
      const blockingCallId = Guid.newGuid().toString();
      const blockingTask = grain.notifyLongWait(30_000, blockingCallId, blockingCts.token);
      await waitFor(() => observer.hasStarted(blockingCallId));

      // A second request queues behind it on the grain's turn scheduler.
      const queuedCts = cluster.newCancellationTokenSource();
      const queuedCallId = Guid.newGuid().toString();
      const queuedTask = grain.notifyLongWait(10_000, queuedCallId, queuedCts.token);

      // Cancel the still-queued request's token right away: upstream Orleans
      // can drop a request still waiting in the activation's incoming queue
      // via message-level cancellation, without ever dispatching it to the
      // grain method. This runtime's `TurnScheduler` has no equivalent
      // queue-level cancellation — only the in-body `GrainCancellationToken`
      // signal — so `queuedTask` cannot settle until its turn is actually
      // admitted, which requires the blocking request to finish first.
      // Cancelling both (rather than asserting the queued one alone first,
      // as upstream does) avoids a deadlock while still proving the same
      // outcome: once admitted with an already-fired signal, the queued
      // request rejects immediately without ever completing the observer's
      // delay.
      await queuedCts.cancel();
      await cancelUntilSettled([
        { cts: blockingCts, task: blockingTask },
        { cts: queuedCts, task: queuedTask },
      ]);

      await expectCancelled(queuedTask);
      await expectCancelled(blockingTask);
      await waitFor(() => observer.wasCancelled(blockingCallId));

      await grain.unsubscribe(ref);
      client.deleteObjectReference(ref);
    });

    orleansTest.each([true, false])(
      `${testClass}.InterleavingObserverTaskCancellation`,
      async (cancelImmediately) => {
        const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
        const observer = makeLongRunningObserver();
        const ref = client.createObjectReference(ILongRunningObserver, observer);
        await grain.subscribe(ref);

        const cts = cluster.newCancellationTokenSource();
        const callId = Guid.newGuid().toString();
        const grainTask = grain.notifyInterleavingLongWait(10_000, callId, cts.token);

        if (!cancelImmediately) await waitFor(() => observer.hasStarted(callId));
        await cancelUntilSettled([{ cts, task: grainTask }]);

        await expectCancelled(grainTask);
        if (!cancelImmediately) await waitFor(() => observer.wasCancelled(callId));
        await waitFor(async () => (await grain.getProcessedCancellations()).includes(callId));

        await grain.unsubscribe(ref);
        client.deleteObjectReference(ref);
      },
    );

    orleansTest(`${testClass}.MultipleInterleavingRequestsCancellation`, async () => {
      const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
      const observer = makeLongRunningObserver();
      const ref = client.createObjectReference(ILongRunningObserver, observer);
      await grain.subscribe(ref);

      const cts1 = cluster.newCancellationTokenSource();
      const cts2 = cluster.newCancellationTokenSource();
      const cts3 = cluster.newCancellationTokenSource();
      const callId1 = Guid.newGuid().toString();
      const callId2 = Guid.newGuid().toString();
      const callId3 = Guid.newGuid().toString();

      const task1 = grain.notifyInterleavingLongWait(10_000, callId1, cts1.token);
      const task2 = grain.notifyInterleavingLongWait(10_000, callId2, cts2.token);
      const task3 = grain.notifyInterleavingLongWait(10_000, callId3, cts3.token);

      await Promise.all([
        waitFor(() => observer.hasStarted(callId1)),
        waitFor(() => observer.hasStarted(callId2)),
        waitFor(() => observer.hasStarted(callId3)),
      ]);

      // Cancel only the second request.
      await cancelUntilSettled([{ cts: cts2, task: task2 }]);
      await expectCancelled(task2);
      await waitFor(() => observer.wasCancelled(callId2));

      // First and third are still running; cancel them too.
      await cancelUntilSettled([
        { cts: cts1, task: task1 },
        { cts: cts3, task: task3 },
      ]);
      await expectCancelled(task1);
      await expectCancelled(task3);
      await waitFor(() => observer.wasCancelled(callId1));
      await waitFor(() => observer.wasCancelled(callId3));

      await grain.unsubscribe(ref);
      client.deleteObjectReference(ref);
    });

    orleansTest(`${testClass}.CancelInterleavingWhileRegularRequestRunning`, async () => {
      const grain = cluster.getGrain(IObserverWithCancellationGrain, randomGuidKey());
      const observer = makeLongRunningObserver();
      const ref = client.createObjectReference(ILongRunningObserver, observer);
      await grain.subscribe(ref);

      // A regular (non-interleaving) request occupies the grain's turn.
      const regularCts = cluster.newCancellationTokenSource();
      const regularCallId = Guid.newGuid().toString();
      const regularTask = grain.notifyLongWait(30_000, regularCallId, regularCts.token);
      await waitFor(() => observer.hasStarted(regularCallId));

      // An interleaving request runs concurrently even though the regular
      // call still occupies the grain's turn.
      const interleavingCts = cluster.newCancellationTokenSource();
      const interleavingCallId = Guid.newGuid().toString();
      const interleavingTask = grain.notifyInterleavingLongWait(
        10_000,
        interleavingCallId,
        interleavingCts.token,
      );
      await waitFor(() => observer.hasStarted(interleavingCallId));

      // Cancel only the interleaving request.
      await cancelUntilSettled([{ cts: interleavingCts, task: interleavingTask }]);
      await expectCancelled(interleavingTask);
      await waitFor(() => observer.wasCancelled(interleavingCallId));

      // Clean up: cancel the regular request too.
      await cancelUntilSettled([{ cts: regularCts, task: regularTask }]);
      await expectCancelled(regularTask);
      await waitFor(() => observer.wasCancelled(regularCallId));

      await grain.unsubscribe(ref);
      client.deleteObjectReference(ref);
    });
  }
});
