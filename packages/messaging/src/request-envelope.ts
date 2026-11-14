import type { InvocationRequest } from "@thresh/core/request";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { Message, RequestContext } from "@thresh/messaging/message";

/** What a sender adds to an `InvocationRequest` to put it on the wire. */
export interface RequestEnvelopeParts {
  correlationId: bigint;
  sendingSilo: SiloAddress;
  /** The serialized arguments. */
  body: Uint8Array;
  /** The sender's clock now: `deadlineInMs` is written relative to it. */
  now: number;
  /**
   * Milliseconds until the sender stops waiting for the reply (Orleans `TimeToLive`). Dropped on
   * a one-way request, which nobody waits for (Orleans `Message.IsExpirableMessage`).
   */
  timeToLiveMs: number;
}

/**
 * The one request envelope. Every sender of a grain call — a silo's remote send, a gateway's
 * delivery to a client it holds, and an external client — builds its message here, so a field
 * added to the request (interface version, forward count, transaction, deadline) reaches every
 * hop rather than only the senders someone remembered to update.
 *
 * The deadline and time-to-live are RELATIVE to the sender's clock, like Orleans' wire
 * `TimeToLive`; the receiver re-bases them on its own (issue #90).
 */
export function requestEnvelope(req: InvocationRequest, parts: RequestEnvelopeParts): Message {
  const oneWay = req.options.oneWay === true;
  const requestContext: RequestContext = {};
  if (req.reentrancyId !== undefined) requestContext.reentrancyId = req.reentrancyId;
  if (req.transaction !== undefined) {
    requestContext.transaction = {
      id: req.transaction.id,
      timeStamp: req.transaction.timeStamp,
      readOnly: req.transaction.readOnly,
    };
  }
  if (req.headers !== undefined) requestContext.headers = req.headers;
  return {
    correlationId: parts.correlationId,
    direction: oneWay ? "oneWay" : "request",
    targetGrain: req.target,
    sendingSilo: parts.sendingSilo,
    ...(req.sender !== undefined ? { sendingGrain: req.sender } : {}),
    ...(req.callingGrain !== undefined ? { callingGrain: req.callingGrain } : {}),
    interfaceId: req.interfaceId,
    ...(req.interfaceVersion !== undefined ? { interfaceVersion: req.interfaceVersion } : {}),
    method: req.method,
    ...(req.deadline !== undefined ? { deadlineInMs: req.deadline - parts.now } : {}),
    ...(oneWay ? {} : { timeToLiveMs: parts.timeToLiveMs }),
    ...(req.forwardCount !== undefined ? { forwardCount: req.forwardCount } : {}),
    requestContext,
    body: parts.body,
  };
}
