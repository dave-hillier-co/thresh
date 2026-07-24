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
   * `AbortSignal`), so it rides a cross-silo forward and each hop derives its
   * own local `AbortSignal` from it (`@tsva/runtime/dispatcher`). Propagated
   * unchanged down a call chain like `transaction`, so the FIRST deadline set
   * on a chain governs every downstream hop.
   */
  deadline?: number;
}
