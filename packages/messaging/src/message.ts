import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { AccessCounter, ParticipantId } from "@tsva/core/transaction-info";

export type Direction = "request" | "response" | "oneWay";
export type ResponseKind = "success" | "error" | "rejection";

/** The serializable transaction context carried on a request (no participant set). */
export interface TransactionContextHeader {
  id: string;
  timeStamp: number;
  readOnly: boolean;
}

/** A participant a callee enlisted, sent back on the reply so the root can merge it. */
export interface SerializedParticipant {
  id: ParticipantId;
  access: AccessCounter;
}

/** Ambient headers propagated along a call chain (trace ids, deadlines, etc.). */
export interface RequestContext {
  reentrancyId?: string | undefined;
  /** The ambient transaction this call participates in. */
  transaction?: TransactionContextHeader | undefined;
  /** Request-context headers (Orleans `RequestContext`): trace context + app baggage. */
  headers?: Record<string, string> | undefined;
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
  /** Caller's interface version for version-aware placement (absent ⇒ 1). */
  interfaceVersion?: number | undefined;
  method: string;

  /** Marks a system request (directory, migration, manifest, load, or rebalance) vs a grain call. */
  system?: "directory" | "migration" | "manifest" | "load" | "rebalance" | undefined;

  responseKind?: ResponseKind | undefined;
  requestContext?: RequestContext | undefined;

  /** On a reply: participants the callee enlisted, for the caller to merge. */
  transactionParticipants?: SerializedParticipant[] | undefined;

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
  const response: Message = {
    correlationId: request.correlationId,
    direction: "response",
    targetGrain: request.targetGrain,
    sendingSilo,
    interfaceId: request.interfaceId,
    method: request.method,
    responseKind,
    body,
  };
  if (request.interfaceVersion !== undefined) response.interfaceVersion = request.interfaceVersion;
  return response;
}
