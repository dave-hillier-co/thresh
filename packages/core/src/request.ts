import type { GrainId } from "./grain-id";
import type { InvokeMethodOptions } from "./invoke-options";
import type { TransactionInfo } from "./transaction-info";

/**
 * The request a grain reference builds for one method call. The dispatcher
 * routes it locally or to the owning silo. In Phase 2 this is wrapped into a
 * `Message` envelope for transport.
 */
export interface InvocationRequest {
  target: GrainId;
  /** Interface id — routes `getGrain` to the hosting type and rehydrates refs. */
  interfaceId: number;
  /** Caller's compiled interface version for version-aware placement (absent ⇒ 1). */
  interfaceVersion?: number;
  /** The method name; the receiving activation dispatches the message by name. */
  method: string;
  args: unknown[];
  options: InvokeMethodOptions;
  /** Call-chain reentrancy id, generated at the root and propagated. */
  reentrancyId: string;
  sender?: GrainId;
  /**
   * The grain activation actually making THIS call (Orleans
   * `Message.SendingGrain`), if any — distinct from `sender`, which carries a
   * propagated caller-chain identity for cascading operations (cancellation
   * forwarding) rather than per-hop provenance. Consulted by the activation
   * repartitioner's message sink to attribute a communication edge to the
   * true immediate caller.
   */
  callingGrain?: GrainId;
  /**
   * Ambient transaction this call participates in, if any. In-process the same
   * `TransactionInfo` flows by reference so resources enlist into it; cross-silo
   * propagation rides the request context (later slice).
   */
  transaction?: TransactionInfo;
  /** Ambient request-context headers propagated along the call chain (Orleans `RequestContext`). */
  headers?: Record<string, string>;
  /**
   * Absolute deadline (epoch ms) for this call chain, if one is ambient
   * (Orleans has no direct analogue — JS-only ambient cancellation; see
   * `docs/deviations.md`). Wire-safe (a plain timestamp, unlike an
   * `AbortSignal`), so it rides a cross-silo forward -- re-based onto each
   * receiving silo's own clock via the wire's relative `deadlineInMs` -- and
   * each hop derives its own local `AbortSignal` from it
   * (`@thresh/runtime/dispatcher`). Propagated
   * unchanged down a call chain like `transaction`, so the FIRST deadline set
   * on a chain governs every downstream hop.
   */
  deadline?: number;
  /**
   * Local-clock instant (ms, on the RECEIVING silo's `TimeProvider`) after
   * which this request must not start running, because its caller has
   * already given up waiting for the reply (Orleans `Message.TimeToLive`,
   * checked in `InsideRuntimeClient.Invoke`). Set by `ClusterNode.toRequest`
   * from the wire `timeToLiveMs`; checked once, at turn start, by
   * `ActivationData.invoke`. Unlike `deadline` it is NOT ambient: calls the
   * turn makes get their own time-to-live from their own caller timeout
   * (issue #90).
   */
  expiresAt?: number;
  /**
   * How many times this exact call has already been forwarded silo-to-silo
   * because the receiving silo's directory CAS named a different owner
   * (Orleans `Message.ForwardCount`). Absent/0 on the caller's original
   * dispatch; incremented by `DistributedDispatcher.forwardTo` on each hop
   * and capped there (`MaxForwardCount`), so an inconsistent directory view
   * cannot loop forever between silos (issue #110).
   */
  forwardCount?: number;
}
