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
import type { GrainCancellationToken } from "@tsva/core/grain-cancellation-token";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";

export interface ILongRunningTaskGrain extends GrainWithGuidKey {
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
}

export const ILongRunningTaskGrain = defineGrainInterface<ILongRunningTaskGrain>(
  "UnitTests.GrainInterfaces.ILongRunningTaskGrain",
  { options: { longWaitGrainCancellationInterleaving: { alwaysInterleave: true } } },
);
