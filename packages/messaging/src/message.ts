import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { AccessCounter, ParticipantId } from "@thresh/core/transaction-info";

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
  /**
   * The grain activation that made THIS specific call (Orleans
   * `Message.SendingGrain` proper), distinct from `sendingGrain` above, which
   * this codebase instead uses to carry a propagated caller-chain identity
   * for cascading operations (see `InvocationContext.senderId`'s doc).
   * Consulted only by the activation repartitioner's message sink.
   */
  callingGrain?: GrainId | undefined;

  interfaceId: number;
  /** Caller's interface version for version-aware placement (absent ⇒ 1). */
  interfaceVersion?: number | undefined;
  method: string;

  /**
   * Marks a system request (directory, migration, manifest, load, stats,
   * detailedstats, actcount, forcecollect, provctl, rebalance, repartition,
   * siloping, or durablejob) or the client-directory gossip (`client`, oneWay only) vs a
   * grain call.
   */
  system?:
    | "directory"
    | "migration"
    | "manifest"
    | "load"
    | "loadstats"
    | "stats"
    | "detailedstats"
    | "actcount"
    | "forcecollect"
    | "provctl"
    | "rebalance"
    | "repartition"
    | "siloping"
    | "durablejob"
    | "client"
    | undefined;

  responseKind?: ResponseKind | undefined;
  requestContext?: RequestContext | undefined;

  /**
   * Milliseconds left, when sent, on the call chain's ambient deadline
   * (`InvocationRequest.deadline`), if one is set. RELATIVE, like Orleans'
   * wire `TimeToLive` (`MessageSerializer` writes the remaining ms and the
   * receiver restarts a local stopwatch from it), so two silos' wall clocks
   * are never compared: `ClusterNode.sendRemote` subtracts its own `now()`,
   * `toRequest` adds the receiver's back (issue #90). May be negative (the
   * deadline had already passed when sent).
   */
  deadlineInMs?: number | undefined;

  /**
   * Orleans `Message.TimeToLive`: milliseconds, when sent, until the caller
   * stops waiting for the reply (its call timeout, or less if the request was
   * itself already carrying a shorter one). Set on every non-one-way request;
   * the receiver turns it into `InvocationRequest.expiresAt` and drops the
   * request at turn start once it has passed, instead of running a call
   * nobody is waiting for any more (issue #90).
   */
  timeToLiveMs?: number | undefined;

  /**
   * How many times this call has already been forwarded silo-to-silo (Orleans
   * `Message.ForwardCount`), carried across a hop so `MaxForwardCount` caps
   * the WHOLE chain, not just one silo's own attempts (issue #110). Absent on
   * a caller's original dispatch.
   */
  forwardCount?: number | undefined;

  /** On a reply: participants the callee enlisted, for the caller to merge. */
  transactionParticipants?: SerializedParticipant[] | undefined;

  /**
   * On a reply: the receiving silo had to forward this call on (its directory
   * CAS named a different owner), so the address the caller had cached/looked
   * up for `targetGrain` is stale (Orleans `MessageCenter
   * .AddToCacheInvalidationHeader`). `ClusterNode.sendRemote` evicts its
   * `LocationCache` entry for `targetGrain` on seeing this instead of routing
   * to the now-wrong silo again next call (issue #110).
   */
  staleCacheEntry?: boolean | undefined;

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
