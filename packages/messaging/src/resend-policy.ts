import { isStaleActivationRejection } from "@thresh/core/errors";
import { ConnectFailure } from "@thresh/messaging/transport";

/**
 * Errors that stand for a request that did not run: the runtime refused it before execution,
 * or it never left this process. Marked at the point that knows (see the two functions below),
 * never inferred from an error's class or kind afterwards.
 */
const didNotRun = new WeakSet<object>();

/**
 * Errors that came out of a turn: a method (or a hosted object) ran and threw them. A mark here
 * wins over `didNotRun`, because the mark there belonged to an earlier, nested request's refusal,
 * not to the request this error now answers: a method that lets a nested call's refusal escape
 * has run, and its caller must not resend it.
 */
const ranInTurn = new WeakSet<object>();

/**
 * Mark `err` as the runtime's refusal of a request before it ran — a routing refusal, an
 * admission refusal, or the hold-and-reroute path's release of a held call. Called only at those
 * refusal points; anything a turn throws is never marked, whatever its class. A refusal crosses
 * the wire as a `rejection` response, and the receiving side marks the error it rebuilds from
 * one. Orleans produces rejections only in `MessageCenter.RejectMessage`.
 */
export function refusedBeforeExecution<E>(err: E): E {
  if (typeof err === "object" && err !== null) didNotRun.add(err);
  return err;
}

/**
 * Mark `err` as thrown out of a turn — called at the turn boundary, where the runtime knows the
 * request was handed to the method. Whatever the error said about an earlier request (a nested
 * call's refusal the method let escape), it now stands for a request that ran.
 */
export function thrownByTurn<E>(err: E): E {
  if (typeof err === "object" && err !== null) ranInTurn.add(err);
  return err;
}

/** Mark `err` as the failure of a send that never put the request on the wire. */
export function neverLeft<E>(err: E): E {
  return refusedBeforeExecution(err);
}

/**
 * Whether `err` stands for a request that did not run: refused before execution, or never left
 * (a send that threw, or a failed dial, which is a `ConnectFailure` from every transport).
 */
export function didNotExecute(err: unknown): boolean {
  if (typeof err !== "object" || err === null || ranInTurn.has(err)) return false;
  return err instanceof ConnectFailure || didNotRun.has(err);
}

/**
 * The one definition of safe-to-resend, used by every dispatcher and the external client. A
 * request is resent only when it did not run (`didNotExecute`) — so a resend can never run it
 * twice — and the refusal says its target moved or is momentarily unknown
 * (`isStaleActivationRejection`), so re-resolving the target can change the outcome: an overload
 * or draining refusal would meet the same answer again. A reply timeout, a connection lost
 * mid-call, and any error a turn threw (a nested call's rejection included) are never resent:
 * the request may have run.
 */
export function mayResend(err: unknown): boolean {
  return didNotExecute(err) && isStaleActivationRejection(err);
}
