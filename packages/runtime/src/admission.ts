import type { InvocationRequest } from "@thresh/core/request";
import { refusedBeforeExecution, thrownByTurn } from "@thresh/messaging/resend-policy";
import type { ActivationData } from "@thresh/runtime/activation";
import type { InvokeCallOptions } from "@thresh/runtime/dispatcher";

/**
 * Hand `req` to `activation`, marking the activation's own refusal of it — a call it held while
 * deactivating or migrating and then released for a reroute, or one it refused as stuck
 * (`ActivationData.isRerouteRejection`) — as refused before execution. Everything else the call
 * fails with came out of its turn and is marked as such (`thrownByTurn`), whatever its class and
 * whatever an earlier refusal said about it, so it is never resent (see `mayResend`).
 *
 * `activation.invoke` runs synchronously up to queueing the turn, as it does when called
 * directly, so a caller that signals "admitted" right after this returns still does so once the
 * turn is queued.
 */
export async function admit(
  activation: ActivationData,
  req: InvocationRequest,
  opts?: InvokeCallOptions,
): Promise<unknown> {
  try {
    return await activation.invoke(req, opts);
  } catch (err) {
    if (activation.isRerouteRejection(err)) refusedBeforeExecution(err);
    // Anything else came out of the turn: an error the method let escape from a nested call
    // carries that call's refusal mark, which must not describe this request.
    else thrownByTurn(err);
    throw err;
  }
}
