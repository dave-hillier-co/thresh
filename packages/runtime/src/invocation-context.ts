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
  /**
   * Ambient request-context headers (Orleans `RequestContext`): a string→string
   * bag that flows along the call chain — carrying W3C trace context and app
   * baggage (tenant, correlation id). Mutable within a turn; the proxy copies it
   * onto outgoing calls.
   */
  headers?: Record<string, string> | undefined;
}

export const invocationContext = new AsyncLocalStorage<InvocationContext>();

/**
 * The ambient request context for the current turn (Orleans `RequestContext`).
 * `set` requires a turn in scope; values flow to downstream grain calls and
 * across silos via the message envelope.
 */
export const requestContext = {
  get(key: string): string | undefined {
    return invocationContext.getStore()?.headers?.[key];
  },
  set(key: string, value: string): void {
    const store = invocationContext.getStore();
    if (store === undefined) {
      throw new Error("requestContext.set must be called within a grain turn");
    }
    (store.headers ??= {})[key] = value;
  },
  getAll(): Record<string, string> {
    return { ...(invocationContext.getStore()?.headers ?? {}) };
  },
};

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
