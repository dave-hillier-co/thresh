import { drainCancellationCallbackErrors } from "@tsva/core/grain-cancellation-token";
import { defineGrainInterface } from "@tsva/core/grain-interface";

/**
 * The cross-silo half of cooperative cancellation (Orleans' cancellation
 * `IGrainExtension`): a caller's `GrainCancellationTokenSource.cancel()`
 * calls this on every recorded target grain, wherever it lives, riding the
 * ordinary `GrainExtension` dispatch substrate (`activation.ts`'s
 * `iface?.extension === true` branch) rather than a bespoke system message.
 */
export interface ICancellationSourcesExtension {
  cancelRemoteToken(tokenId: string): Promise<void>;
}

// `cancelRemoteToken` must be able to preempt whatever turn is currently
// running on the target activation — that's the entire point of cancelling a
// long-running call — so it needs `alwaysInterleave: true`. Without this, the
// scheduler (`TurnScheduler`, single-threaded turns per activation) would
// queue the cancel notification strictly behind the very turn it's meant to
// abort, and it would never be admitted until that turn finished on its own
// (see the report for how this was diagnosed via a real cross-silo test).
export const ICancellationSourcesExtension = defineGrainInterface<ICancellationSourcesExtension>(
  "$cancellation",
  { extension: true, options: { cancelRemoteToken: { alwaysInterleave: true } } },
);

/**
 * Per-activation store of `AbortController`s for cancellation tokens that
 * have been bound on this activation (see `ActivationData`'s placeholder
 * binding), keyed by `tokenId`.
 *
 * `cancelRemoteToken` creates-then-aborts rather than looking up-and-maybe-
 * missing: a cancel can race a call carrying the same token and arrive
 * first (cancel-before-arrival), in which case the controller created here
 * is exactly the one the later-bound `GrainCancellationToken` picks up
 * already aborted (see the `placeholder.cancelled` flag in `activation.ts`,
 * which covers the case where the *call* itself arrives after `cancel()`
 * already ran — this covers the case where cancellation and call race on
 * the wire and cancellation's message wins).
 */
export class CancellationSourcesExtension implements ICancellationSourcesExtension {
  private readonly controllers = new Map<string, AbortController>();

  getOrCreateController(tokenId: string): AbortController {
    const existing = this.controllers.get(tokenId);
    if (existing !== undefined) return existing;
    const created = new AbortController();
    this.controllers.set(tokenId, created);
    return created;
  }

  async cancelRemoteToken(tokenId: string): Promise<void> {
    const controller = this.getOrCreateController(tokenId);
    controller.abort();
    // Aborting fires this activation's `GrainCancellationToken.register`
    // callbacks synchronously (they share the controller's signal). A callback
    // that threw was captured rather than allowed to crash the worker; surface
    // it here so it propagates back through `cancel()` to the canceller —
    // Orleans' "a GrainCancellationToken callback's exception flows to
    // Cancel()". The first captured error is thrown (the ported tests register
    // a single callback); any others would be swallowed, matching the fact
    // that the canceller only observes one failure.
    const errors = drainCancellationCallbackErrors(controller.signal);
    const first = errors[0];
    if (first !== undefined) throw first;
  }
}

/** Auto-install factory, always registered by the catalog (see `catalog.ts`). */
export const cancellationExtensionFactory = (): CancellationSourcesExtension =>
  new CancellationSourcesExtension();
