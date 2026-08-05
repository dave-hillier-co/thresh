// Ported from dotnet/orleans test/Grains/TestGrains/GenericGrains.cs (LongRunningTaskGrain<T>) and
// test/Grains/TestGrainInterfaces/GenericTestGrainInterfaces.cs (ILongRunningTaskGrain<T>) @ v10.1.0 (MIT).
//
// Upstream's `ILongRunningTaskGrain<T>` is an open generic grain interface
// (GAP-GENERIC-GRAINS — open generics are unrepresentable here) with methods
// for a non-cancellation-token `CancellationToken`, token-callback resolution,
// and a `WatchCancellations` async-stream observer. This port keeps only the
// cooperative-cancellation subset (`GrainCancellationToken`) exercised by
// `GrainCancellationTokenTests`, specialised to a concrete non-generic
// interface (the tests only ever instantiate `ILongRunningTaskGrain<bool>` and
// never observe the `T` return value in the cancellation cases). `wasCancelled`
// replaces upstream's `WatchCancellations` channel/async-stream with a simple
// poll, since the ported tests only need to know whether a given callId was
// recorded cancelled, not to observe a live stream of cancellations.
import type { GrainCancellationToken } from "@thresh/core/grain-cancellation-token";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Guid } from "@thresh/core/guid";

export interface ILongRunningTaskGrain extends GrainKey<Guid> {
  /** Awaits a cancellable delay; on cancellation records `callId` and throws `GrainTaskCanceledError`. */
  longWaitGrainCancellation(
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void>;
  /** Same body as `longWaitGrainCancellation`, but `[AlwaysInterleave]` so concurrent calls interleave. */
  longWaitGrainCancellationInterleaving(
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void>;
  /** Grain-to-grain: forwards the token to `target.longWaitGrainCancellation`. */
  callOtherLongRunningTaskGrainCancellation(
    target: ILongRunningTaskGrain,
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void>;
  /** Whether `callId` was recorded cancelled. */
  wasCancelled(callId: string): Promise<boolean>;
  /** Whether `callId`'s call has started (registered its cancellation callback). */
  wasStarted(callId: string): Promise<boolean>;

  /**
   * Registers a cancellation callback that records `callId` then throws, and
   * awaits a cancellable delay. The thrown callback exception propagates back
   * to the caller of `GrainCancellationTokenSource.cancel()` (Orleans
   * `GrainCancellationTokenTests.CancellationTokenCallbacksThrow_ExceptionShouldBePropagated`).
   */
  grainCancellationTokenCallbackThrow(token: GrainCancellationToken, callId: string): Promise<void>;
  /**
   * Registers a cancellation callback that records `callId` then throws, and
   * awaits a cancellable delay. Unlike `grainCancellationTokenCallbackThrow`,
   * the callback's exception is contained and does NOT propagate to the
   * canceller (Orleans `CancellationTokenTests
   * .CancellationTokenCallbacksThrow_ExceptionDoesNotPropagate`, where a plain
   * `CancellationToken` callback runs fire-and-forget).
   */
  cancellationTokenCallbackThrow(token: GrainCancellationToken, callId: string): Promise<void>;

  // "Plain CancellationToken" surface, ported from `CancellationTokenTests.cs`.
  // .NET distinguishes a plain `CancellationToken` from a `GrainCancellationToken`;
  // JS has only `GrainCancellationToken` (there is no separate plain-token
  // type here), so these methods are behaviourally identical to their
  // `...GrainCancellation` siblings above — same cooperative-cancellation
  // mechanism, just named after the upstream methods that exercise it.
  /** Awaits a cancellable delay; on cancellation records `callId` and throws `GrainTaskCanceledError`. */
  longWait(token: GrainCancellationToken, delayMs: number, callId: string): Promise<void>;
  /** Same body as `longWait`, but `[AlwaysInterleave]` so concurrent calls interleave. */
  longWaitInterleaving(
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void>;
  /** Grain-to-grain: forwards the token to `target.longWait`. */
  callOtherLongRunningTask(
    target: ILongRunningTaskGrain,
    token: GrainCancellationToken,
    delayMs: number,
    callId: string,
  ): Promise<void>;
}

export const ILongRunningTaskGrain = defineGrainInterface<ILongRunningTaskGrain>(
  "UnitTests.GrainInterfaces.ILongRunningTaskGrain",
  {
    options: {
      longWaitGrainCancellationInterleaving: { alwaysInterleave: true },
      longWaitInterleaving: { alwaysInterleave: true },
      // Orleans' cancellation-state observer (WatchCancellations) is
      // [AlwaysInterleave] so it can be read while a call still occupies the
      // grain's turn; these read-only queries mirror that.
      wasCancelled: { alwaysInterleave: true },
      wasStarted: { alwaysInterleave: true },
    },
  },
);
