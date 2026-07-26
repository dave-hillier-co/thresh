import { drainCancellationCallbackErrors } from "@thresh/core/grain-cancellation-token";
import type { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";

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
  /**
   * Grains THIS activation forwarded a token on to, keyed by `tokenId` then
   * `grainId.toString()` (dedupe, same shape as
   * `GrainCancellationTokenSource.targets`). Populated by
   * `recordForwardTarget` when the activation's own `GrainCancellationToken`
   * (bound in `bindCancellationTokens`) is passed as an argument to a further
   * outgoing call — the hop that makes cancellation cascade past this
   * activation to whichever grain it forwarded the token to.
   */
  private readonly forwardedTargets = new Map<string, Map<string, GrainId>>();
  /** `tokenId`s already cancelled here, so a re-delivered/looping cancel is a no-op (idempotent, cycle-safe). */
  private readonly cancelled = new Set<string>();

  constructor(private readonly canceller: (target: GrainId, tokenId: string) => Promise<void>) {}

  getOrCreateController(tokenId: string): AbortController {
    const existing = this.controllers.get(tokenId);
    if (existing !== undefined) return existing;
    const created = new AbortController();
    this.controllers.set(tokenId, created);
    return created;
  }

  /** Record that this activation forwarded `tokenId` on to `target`, so `cancelRemoteToken` cascades there too. */
  recordForwardTarget(tokenId: string, target: GrainId): void {
    let targets = this.forwardedTargets.get(tokenId);
    if (targets === undefined) {
      targets = new Map();
      this.forwardedTargets.set(tokenId, targets);
    }
    targets.set(target.toString(), target);
  }

  /**
   * Abort this activation's own bound copy of `tokenId`, then cascade to
   * every grain this activation forwarded it to — the mechanism that lets a
   * cancellation reach a token forwarded through a second (or further) hop.
   * Idempotent (`cancelled` guards re-entry): a token already cancelled here
   * returns immediately without re-cascading, which also makes a forwarding
   * cycle (A forwards to B, B forwards back to A) terminate rather than loop.
   */
  async cancelRemoteToken(tokenId: string): Promise<void> {
    if (this.cancelled.has(tokenId)) return;
    this.cancelled.add(tokenId);
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
    const targets = this.forwardedTargets.get(tokenId);
    await Promise.all(
      targets === undefined ? [] : [...targets.values()].map((t) => this.canceller(t, tokenId)),
    );
    const first = errors[0];
    if (first !== undefined) throw first;
  }
}

/** Auto-install factory, always registered by the catalog (see `catalog.ts`). */
export const cancellationExtensionFactory = (
  canceller: (target: GrainId, tokenId: string) => Promise<void>,
): CancellationSourcesExtension => new CancellationSourcesExtension(canceller);
