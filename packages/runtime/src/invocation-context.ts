import { AsyncLocalStorage } from "node:async_hooks";
import type { GrainId } from "@tsva/core/grain-id";

/**
 * Ambient context for the turn currently executing on an activation. A grain
 * reference invoked during a turn reads this to propagate the caller's identity
 * and the call-chain reentrancy id onto the outgoing request.
 */
export interface InvocationContext {
  senderId: GrainId | undefined;
  reentrancyId: string;
}

export const invocationContext = new AsyncLocalStorage<InvocationContext>();
