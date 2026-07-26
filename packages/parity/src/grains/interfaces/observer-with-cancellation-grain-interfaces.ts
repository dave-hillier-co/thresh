// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ILongRunningObserver.cs @ v10.1.0 (MIT).
//
// Upstream also declares `CancellationTokenCallbackResolve` (a token-callback
// registration surface) on both interfaces below and `ClearProcessedCancellations`
// on `IObserverWithCancellationGrain`; neither is exercised by any test we port
// here (`CancellationTokenCallbacksExecutionContext` is .NET-only and excluded,
// see `observer-cancellation-token-tests.test.ts`), so both are left out rather
// than ported as dead surface — GAP-OBSERVERS if a later test needs them.
import type { GrainCancellationToken } from "@thresh/core/grain-cancellation-token";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidKey, GrainWithStringKey } from "@thresh/core/key-kinds";

/**
 * A client-hosted observer supporting long-running, cancellable operations
 * (Orleans `ILongRunningObserver : IGrainObserver`). Unlike a typical
 * observer notification, these methods are two-way: the grain awaits the
 * client's completion (or cancellation) rather than firing-and-forgetting.
 */
export interface ILongRunningObserver extends GrainWithStringKey {
  longWait(delayMs: number, callId: string, token: GrainCancellationToken): Promise<void>;
  /** Same body as `longWait`, but `[AlwaysInterleave]` upstream so concurrent calls interleave. */
  interleavingLongWait(delayMs: number, callId: string, token: GrainCancellationToken): Promise<void>;
}

export const ILongRunningObserver = defineGrainInterface<ILongRunningObserver>(
  "UnitTests.GrainInterfaces.ILongRunningObserver",
  { options: { interleavingLongWait: { alwaysInterleave: true } } },
);

/**
 * A grain that forwards long-running, cancellable notifications to a
 * subscribed client-hosted `ILongRunningObserver` (Orleans
 * `IObserverWithCancellationGrain`).
 */
export interface IObserverWithCancellationGrain extends GrainWithGuidKey {
  subscribe(observer: ILongRunningObserver): Promise<void>;
  unsubscribe(observer: ILongRunningObserver): Promise<void>;
  notifyLongWait(delayMs: number, callId: string, token: GrainCancellationToken): Promise<void>;
  /** `[AlwaysInterleave]` upstream: runs concurrently with other requests. */
  notifyInterleavingLongWait(
    delayMs: number,
    callId: string,
    token: GrainCancellationToken,
  ): Promise<void>;
  /**
   * The callIds recorded cancelled so far. Upstream does not mark this
   * `[AlwaysInterleave]` (it never needs to read it while a notify call is
   * still in flight), but our port's tests poll it via `waitFor` while a
   * concurrent notify call may still occupy the grain's turn (mirroring
   * `wasCancelled` on `ILongRunningTaskGrain`) — without this it would
   * deadlock behind a still-running regular call.
   */
  getProcessedCancellations(): Promise<string[]>;
}

export const IObserverWithCancellationGrain = defineGrainInterface<IObserverWithCancellationGrain>(
  "UnitTests.GrainInterfaces.IObserverWithCancellationGrain",
  {
    options: {
      notifyInterleavingLongWait: { alwaysInterleave: true },
      getProcessedCancellations: { alwaysInterleave: true },
    },
  },
);
