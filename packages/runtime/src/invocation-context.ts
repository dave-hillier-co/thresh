import { AsyncLocalStorage } from "node:async_hooks";
import type { GrainId } from "@tsva/core/grain-id";
import { RequestContext, requestContextStore } from "@tsva/core/request-context";
import type { TransactionInfo } from "@tsva/core/transaction-info";

/**
 * Ambient context for the turn currently executing on an activation. A grain
 * reference invoked during a turn reads this to propagate the caller's identity
 * and the call-chain reentrancy id onto the outgoing request.
 *
 * The request-context headers bag itself is NOT stored here — it lives in
 * `@tsva/core`'s `RequestContext`, the same ambient store a non-grain (client)
 * caller uses. A turn scopes a fresh copy into that store (see `activation.ts`
 * / `client-node.ts`, which wrap `invocationContext.run` in
 * `runWithRequestContext`), so `requestContext.get`/`set` below and
 * `RequestContext.get`/`set` read/write the identical bag during a turn.
 */
export interface InvocationContext {
  senderId: GrainId | undefined;
  reentrancyId: string;
  /** The transaction this turn runs inside, if any. */
  transaction?: TransactionInfo | undefined;
}

export const invocationContext = new AsyncLocalStorage<InvocationContext>();

/**
 * The ambient request context for the current turn (Orleans `RequestContext`).
 * `set` requires a turn in scope; values flow to downstream grain calls and
 * across silos via the message envelope. Backed by `@tsva/core`'s
 * `RequestContext` — the same ambient store a non-grain (client) caller uses,
 * so a client-set header and a grain-set header are the same mechanism.
 */
export const requestContext = {
  get(key: string): string | undefined {
    return RequestContext.get(key);
  },
  set(key: string, value: string): void {
    if (invocationContext.getStore() === undefined) {
      throw new Error("requestContext.set must be called within a grain turn");
    }
    RequestContext.set(key, value);
  },
  getAll(): Record<string, string> {
    return { ...(requestContextStore() ?? {}) };
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
