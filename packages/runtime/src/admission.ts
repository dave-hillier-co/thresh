import type { InvocationRequest } from "@thresh/core/request";
import { refusedBeforeExecution } from "@thresh/messaging/resend-policy";
import type { ActivationData } from "@thresh/runtime/activation";
import type { InvokeCallOptions } from "@thresh/runtime/dispatcher";

/**
 * Hand `req` to `activation`, marking the activation's own refusal of it — a call it held while
 * deactivating or migrating and then released for a reroute, or one it refused as stuck
 * (`ActivationData.isRerouteRejection`) — as refused before execution. Everything else the call
 * fails with came out of its turn and is left unmarked, whatever its class, so it is never
 * resent (see `mayResend`).
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
    throw err;
  }
}
