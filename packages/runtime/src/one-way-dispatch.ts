import type { Logger } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";

/**
 * The single chokepoint both `LocalDispatcher.invoke` and
 * `DistributedDispatcher.invoke` route through, so a `oneWay` call is
 * fire-and-forget wherever placement happened to put the callee.
 *
 * Orleans' `[OneWay]` completes the caller's task as soon as the message is
 * handed to the messaging layer, and the runtime carries the message the rest
 * of the way — routing, activation, execution and any failure — off the
 * caller's task entirely (`InsideRuntimeClient.SendRequest`; the response is
 * never awaited, and `OneWay` messages get no callback registered). Our remote
 * path already behaved that way (`ClusterNode.sendRemote` returns right after
 * `conn.send`), but every LOCAL delivery awaited `ActivationData.invoke` — so
 * the very same `await grain.fireAndForget()` blocked for the callee's whole
 * turn when placement landed on the calling silo and returned immediately when
 * it didn't: a location-transparency break Orleans does not have.
 *
 * `deliver` is therefore called (synchronously — see below) and then simply
 * dropped for a `oneWay` request. Deliberately wrapping the WHOLE delivery,
 * not just the final `activation.invoke`, so everything the runtime does on
 * the message's behalf — directory lookup, placement, the stale-entry
 * invalidate-and-re-resolve retry in `DistributedDispatcher.invoke`, a forward
 * to the CAS winner — still runs to completion; it just runs detached, like
 * Orleans' messaging pipeline.
 *
 * Two properties this relies on:
 *
 * - `deliver` is CALLED synchronously here (its body runs up to its own first
 *   `await` before this function returns), and every one-way call to a given
 *   grain traverses the same delivery path, hence the same number of microtask
 *   hops, before reaching `ActivationData.invoke` — which enqueues its turn
 *   synchronously, before its own first `await` (`TurnScheduler.schedule`
 *   pushes inside the promise executor). Successive one-way calls therefore
 *   still reach the queue in call order. Note what that does and does not
 *   cover: `LocalDispatcher.deliver` and `DistributedDispatcher.deliver` both
 *   `await` a catalog lookup first, and the ordering survives only because
 *   those lookups (`Catalog.getOrActivate`/`resolveLive`) are deliberately NOT
 *   `async` and return an already-resolved promise — see the comment on
 *   `Catalog.getOrActivate`, which relies on the same property for its own
 *   check-then-set. Where the path genuinely diverges per call — a grain not
 *   yet activated, whose first claim must await a directory CAS —
 *   `DistributedDispatcher` restores the ordering explicitly by admitting one
 *   one-way claim at a time per grain id
 *   (`DistributedDispatcher.claimingOneWay`), because detaching removed the
 *   awaiting caller that used to serialize those. Detaching also keeps
 *   `Catalog.pickOrScaleWorker`'s "invoke the returned activation
 *   synchronously" contract intact.
 * - the detached promise is given a `catch`, so a callee that throws is logged
 *   rather than crashing the process as an unhandled rejection — the same
 *   catch-and-log the runtime already applies to other detached work
 *   (`ActivationData.beginActivate`, and `GrainTimerImpl.fire` mirroring
 *   Orleans' `TimerQueueTimer.TimerTick`). The caller cannot be told: it has
 *   no response channel by definition, which is exactly Orleans' behaviour for
 *   a `[OneWay]` method that faults.
 *
 * Because the callee's turn is no longer inside the caller's happens-before
 * chain, `oneWay` is also incompatible with a transaction boundary: the root
 * would resolve the transaction before the callee had contributed to it.
 * `GrainFactory.resolveTransaction` rejects that declaration outright.
 */
export function dispatchDetachingOneWay(
  req: InvocationRequest,
  logger: Logger,
  deliver: () => Promise<unknown>,
): Promise<unknown> {
  const delivery = deliver();
  if (req.options.oneWay !== true) return delivery;
  delivery.catch((error: unknown) => {
    logger.warn("one-way grain call failed", {
      grainId: req.target.toString(),
      method: req.method,
      error,
    });
  });
  return Promise.resolve(undefined);
}
