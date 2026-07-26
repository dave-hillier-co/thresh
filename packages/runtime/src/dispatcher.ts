import type { InvocationRequest } from "@thresh/core/request";

/**
 * Local, per-call ambient cancellation for a `Dispatcher.invoke` — distinct
 * from `InvocationRequest.deadline`, which is wire-safe and rides a
 * cross-silo forward; an `AbortSignal` cannot cross the wire, so `signal`
 * only ever governs delivery that stays on THIS process (a local activation,
 * or the local leg of a call this silo happens to place remotely — see
 * `DistributedDispatcher`). Orleans has no analogue (JS-only ambient
 * cancellation; see `docs/deviations.md`).
 */
export interface InvokeCallOptions {
  /** Ambient cancellation signal for this call; composes with `deadlineMs` (`combineSignals`). */
  signal?: AbortSignal;
  /**
   * A fresh per-call deadline, in ms from now, applied only when `req` does
   * not already carry one (an earlier hop's deadline always wins — see
   * `InvocationRequest.deadline`). Converted to an absolute epoch-ms deadline
   * with the real wall clock (`Date.now()`) so it survives a cross-silo
   * forward unchanged, regardless of which silo's fake/real clock a test
   * might be driving locally.
   */
  deadlineMs?: number;
}

/**
 * Routes a grain call to its target. Phase 1 dispatches locally; Phase 2
 * resolves the owning silo via the directory and forwards over the transport.
 */
export interface Dispatcher {
  invoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown>;
}

/**
 * Resolve `opts.deadlineMs` into `req.deadline`, the single chokepoint both
 * `LocalDispatcher` and `DistributedDispatcher` call at the top of `invoke` —
 * so a fresh per-call deadline is embedded before any placement/forwarding
 * decision, and therefore rides a distributed forward too. A no-op when
 * `opts.deadlineMs` is absent, or `req` already carries a deadline (an
 * earlier hop's wins).
 */
export function withCallDeadline(
  req: InvocationRequest,
  opts: InvokeCallOptions | undefined,
): InvocationRequest {
  if (opts?.deadlineMs === undefined || req.deadline !== undefined) return req;
  return { ...req, deadline: Date.now() + opts.deadlineMs };
}
