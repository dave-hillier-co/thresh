import { AsyncLocalStorage } from "node:async_hooks";
import { combineSignals } from "@thresh/core/abort";
import type { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";
import { RequestContext, requestContextStore } from "@thresh/core/request-context";
import type { TransactionInfo } from "@thresh/core/transaction-info";

/**
 * Ambient context for the turn currently executing on an activation. A grain
 * reference invoked during a turn reads this to propagate the caller's identity
 * and the call-chain reentrancy id onto the outgoing request.
 *
 * The request-context headers bag itself is NOT stored here — it lives in
 * `@thresh/core`'s `RequestContext`, the same ambient store a non-grain (client)
 * caller uses. A turn scopes a fresh copy into that store (see `activation.ts`
 * / `client-node.ts`, which wrap `invocationContext.run` in
 * `runWithRequestContext`), so `requestContext.get`/`set` below and
 * `RequestContext.get`/`set` read/write the identical bag during a turn.
 */
export interface InvocationContext {
  /**
   * Propagated caller-chain identity (Orleans-adjacent name, but NOT message
   * provenance): carried through a call chain unchanged from wherever it was
   * first set, so a cascading operation (e.g. `GrainCancellationToken`
   * forwarding, `grain-factory.ts`'s `source: ambient?.senderId`) can trace
   * back to that origin. NOT "who called me for this specific hop" — see
   * `ownerId` for that.
   */
  senderId: GrainId | undefined;
  /**
   * This turn's own activation id — the grain actually executing right now.
   * Read by a grain reference invoked during the turn to stamp the outgoing
   * request's `callingGrain` (Orleans `Message.SendingGrain`): the immediate
   * caller for THIS hop, unlike `senderId` above.
   */
  ownerId: GrainId | undefined;
  reentrancyId: string;
  /** The transaction this turn runs inside, if any. */
  transaction?: TransactionInfo | undefined;
  /**
   * Absolute deadline (epoch ms) ambient for this call chain, if any —
   * propagated onto an outgoing request the same way `transaction` is (see
   * `grain-factory.ts`), so it rides every downstream hop until something
   * sets a fresh one. Orleans has no analogue (JS-only ambient cancellation;
   * see `docs/deviations.md`).
   */
  deadline?: number | undefined;
  /**
   * This turn's ambient cancellation signal, if any — set by
   * `ActivationData.invoke` from the caller's `InvokeCallOptions.signal`/
   * `deadline`. Propagated AS-IS to the next hop's `InvokeCallOptions.signal`
   * (`grain-factory.ts`) and is what `TurnScheduler` uses to preempt a
   * still-queued turn. Deliberately NOT combined with `tokenSignals` below —
   * see its doc for why the two must stay separate.
   */
  signal?: AbortSignal | undefined;
  /**
   * Signals from `GrainCancellationToken`s bound during this turn
   * (`ActivationData.bindCancellationTokens`) — composed into
   * `GrainRuntime.getCancellationSignal()`'s result (see `currentSignal`) but
   * deliberately kept OUT of `signal` above and never propagated to an
   * outgoing call: `GrainCancellationToken` is purely cooperative (Orleans
   * parity — a callee must explicitly observe it), so it must never cause
   * `TurnScheduler` to preemptively reject a downstream call at admission the
   * way a genuine ambient `signal`/deadline does. Composing it only into the
   * turn-local observable signal keeps `getCancellationSignal()` convenient
   * without smuggling that admission-preempting power onto every call a
   * cancellation-token-carrying grain happens to make afterward.
   */
  tokenSignals?: AbortSignal[];
}

export const invocationContext = new AsyncLocalStorage<InvocationContext>();

/**
 * The ambient request context for the current turn (Orleans `RequestContext`).
 * `set` requires a turn in scope; values flow to downstream grain calls and
 * across silos via the message envelope. Backed by `@thresh/core`'s
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

/**
 * The current turn's cancellation signal for grain code to observe
 * cooperatively — the ambient `InvokeCallOptions.signal`/deadline-derived
 * `signal` composed with any bound `GrainCancellationToken`s'
 * `tokenSignals`. NOT the same value `TurnScheduler`/propagation to the next
 * hop use (that's `signal` alone) — see `InvocationContext.tokenSignals`.
 */
export function currentSignal(): AbortSignal | undefined {
  const store = invocationContext.getStore();
  if (store === undefined) return undefined;
  return combineSignals([store.signal, ...(store.tokenSignals ?? [])]);
}

/** The current turn's ambient deadline (epoch ms), or `undefined` if none is in scope. */
export function currentDeadline(): number | undefined {
  return invocationContext.getStore()?.deadline;
}

/** Options for {@link withCallOptions}: a caller-chosen deadline and/or cancellation signal. */
export interface CallOptions {
  /**
   * A fresh deadline, in ms from now, for every grain call made inside `fn` —
   * overriding any ambient deadline already in scope (an explicit per-call
   * override always wins, unlike `InvokeCallOptions.deadlineMs`'s
   * fill-in-if-absent default). Converted to an absolute epoch-ms deadline
   * with the real wall clock, same as `InvokeCallOptions.deadlineMs`, so it
   * survives a cross-silo forward.
   */
  deadlineMs?: number;
  /** An additional cancellation signal, composed with any ambient one already in scope. */
  signal?: AbortSignal;
}

/**
 * Run `fn` with a caller-chosen deadline/signal in ambient scope for every
 * grain call it makes — the "friendlier per-call deadline API" `GrainFactory`
 * itself has no room for (it only propagates whatever `InvocationContext`
 * already carries; it never lets a caller set a NEW one). Usable both inside
 * a grain turn (nests under whatever ambient deadline/signal is already
 * running — `deadlineMs` overrides it, `signal` composes with it) and from
 * top-level (non-grain) client code, which otherwise has no ambient
 * `InvocationContext` at all.
 *
 * Because `GrainFactory`'s proxy already reads `deadline`/`signal` off this
 * same ambient store (see `grain-factory.ts`) and stamps every outgoing
 * `InvocationRequest`/`InvokeCallOptions` from it, and a callee's own turn
 * re-derives its ambient context from those (`ActivationData.invoke`), the
 * deadline/signal set here automatically rides the WHOLE downstream call
 * chain — no `grain-factory.ts` changes needed.
 */
export function withCallOptions<T>(options: CallOptions, fn: () => Promise<T>): Promise<T> {
  const current = invocationContext.getStore();
  const next: InvocationContext = {
    senderId: current?.senderId,
    ownerId: current?.ownerId,
    reentrancyId: current?.reentrancyId ?? Guid.newGuid().toString(),
    transaction: current?.transaction,
    deadline:
      options.deadlineMs !== undefined ? Date.now() + options.deadlineMs : current?.deadline,
    signal: combineSignals([current?.signal, options.signal]),
    ...(current?.tokenSignals !== undefined ? { tokenSignals: current.tokenSignals } : {}),
  };
  return invocationContext.run(next, fn);
}
