import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";

export type Direction = "request" | "response" | "oneWay";
export type ResponseKind = "success" | "error" | "rejection";

/** Ambient headers propagated along a call chain (trace ids, deadlines, etc.). */
export interface RequestContext {
  reentrancyId?: string | undefined;
  [key: string]: unknown;
}

/** The envelope carrying one grain call or its result across the transport. */
export interface Message {
  correlationId: bigint;
  direction: Direction;

  targetGrain: GrainId;
  targetSilo?: SiloAddress | undefined;
  sendingGrain?: GrainId | undefined;
  sendingSilo?: SiloAddress | undefined;

  interfaceId: number;
  methodId: number;

  responseKind?: ResponseKind | undefined;
  requestContext?: RequestContext | undefined;

  /** Serialized arguments (request) or result/error (response). */
  body: Uint8Array;
}

let counter = 0n;

/** Monotonic correlation id for matching a response to its request. */
export function nextCorrelationId(): bigint {
  return ++counter;
}

/** Build a response message that mirrors a request's routing fields. */
export function responseTo(
  request: Message,
  responseKind: ResponseKind,
  body: Uint8Array,
  sendingSilo?: SiloAddress,
): Message {
  return {
    correlationId: request.correlationId,
    direction: "response",
    targetGrain: request.targetGrain,
    sendingSilo,
    interfaceId: request.interfaceId,
    methodId: request.methodId,
    responseKind,
    body,
  };
}
