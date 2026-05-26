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
  /** The method name; the receiving activation dispatches the message by name. */
  method: string;
  args: unknown[];
  options: InvokeMethodOptions;
  /** Call-chain reentrancy id, generated at the root and propagated. */
  reentrancyId: string;
  sender?: GrainId;
  /**
   * Ambient transaction this call participates in, if any. In-process the same
   * `TransactionInfo` flows by reference so resources enlist into it; cross-silo
   * propagation rides the request context (later slice).
   */
  transaction?: TransactionInfo;
  /** Ambient request-context headers propagated along the call chain (Orleans `RequestContext`). */
  headers?: Record<string, string>;
}
