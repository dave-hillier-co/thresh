import { AsyncLocalStorage } from "node:async_hooks";
import type { GrainId } from "@tsva/core/grain-id";
import type { TransactionInfo } from "@tsva/core/transaction-info";

/**
 * Ambient context for the turn currently executing on an activation. A grain
 * reference invoked during a turn reads this to propagate the caller's identity
 * and the call-chain reentrancy id onto the outgoing request.
 */
export interface InvocationContext {
  senderId: GrainId | undefined;
  reentrancyId: string;
  /** The transaction this turn runs inside, if any (Phase 7, ADR 0008). */
  transaction?: TransactionInfo | undefined;
}

export const invocationContext = new AsyncLocalStorage<InvocationContext>();

/** The transaction the current turn runs inside, or `undefined` outside one. */
export function currentTransaction(): TransactionInfo | undefined {
  return invocationContext.getStore()?.transaction;
}

/** The current transaction, or throw — used by transactional state on write. */
export function requireTransaction(): TransactionInfo {
  const tx = currentTransaction();
  if (tx === undefined) {
    throw new Error("operation requires a transaction but none is in scope");
  }
  return tx;
}
