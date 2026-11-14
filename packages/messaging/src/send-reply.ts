import { recordReplyFailure } from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
import type { Connection } from "@thresh/messaging/transport";

/**
 * Send `response` down `connection`, the connection its request arrived on. When it cannot be
 * sent (no arrival connection, or the connection has closed) the failure is counted
 * (`thresh.messaging.reply.failed`) and returned: the request has already run, so there is
 * nothing to retry, but a lost reply is never silent. The caller decides whether to log it too.
 */
export function sendReply(
  connection: Connection | undefined,
  request: Message,
  response: Message,
): { failure: unknown } | undefined {
  try {
    if (connection === undefined) throw new Error("the request did not arrive on a connection");
    connection.send(response);
    return undefined;
  } catch (failure) {
    recordReplyFailure({ "thresh.message.system": request.system ?? "grain" });
    return { failure };
  }
}
