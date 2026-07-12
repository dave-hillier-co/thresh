import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Orleans `RequestContext`: an ambient string→string bag that rides a call
 * chain — W3C trace context, tenant/correlation ids, and other app baggage a
 * caller wants a callee to see without threading it through every method
 * signature. Backed by a single {@link AsyncLocalStorage}, shared by both a
 * grain turn (`@tsva/runtime`'s `requestContext` delegates here, scoping a
 * fresh store per turn via {@link runWithRequestContext}) and a non-grain
 * caller (a client / test, calling {@link RequestContext.set} directly,
 * below) — mirroring .NET's `RequestContext`, which is the same static class
 * whether called from grain code or client code.
 */
const storage = new AsyncLocalStorage<Record<string, string>>();

/**
 * The ambient request-context bag for the current async execution context.
 * Call {@link RequestContext.set} from anywhere — grain code mid-turn, or a
 * plain client/test caller with no turn in scope at all — and it flows onto
 * the next outgoing grain call.
 */
export const RequestContext = {
  get(key: string): string | undefined {
    return storage.getStore()?.[key];
  },
  /**
   * Set a value in the ambient bag. Inside an existing scope (a grain turn,
   * or after a prior `set` on this same caller) this mutates that scope's
   * store in place. Outside any scope — a client caller that never opened
   * one — this establishes an ambient store via `enterWith`, so the caller
   * needs no explicit `run`/callback wrapping: it behaves like .NET's
   * ambient `AsyncLocal`, where a bare `RequestContext.Set` "just works"
   * from ordinary (non-grain) code.
   */
  set(key: string, value: string): void {
    const store = storage.getStore();
    if (store !== undefined) {
      store[key] = value;
      return;
    }
    storage.enterWith({ [key]: value });
  },
  remove(key: string): void {
    const store = storage.getStore();
    if (store !== undefined) delete store[key];
  },
  /** Clear every key from the ambient bag, if one is in scope. */
  clear(): void {
    const store = storage.getStore();
    if (store === undefined) return;
    for (const key of Object.keys(store)) delete store[key];
  },
  /** The keys currently set in the ambient bag (Orleans `RequestContext.Keys`). */
  keys(): string[] {
    return Object.keys(storage.getStore() ?? {});
  },
};

/**
 * Run `fn` with `headers` installed as the ambient bag for its async extent —
 * used by the runtime to scope a fresh, mutable copy of the incoming
 * headers to a single grain turn (or client-hosted-object dispatch), so a
 * `RequestContext.set` during that turn does not leak into a sibling or
 * later turn.
 */
export function runWithRequestContext<T>(headers: Record<string, string>, fn: () => T): T {
  return storage.run(headers, fn);
}

/** The raw ambient bag for the current async execution context, if any. */
export function requestContextStore(): Record<string, string> | undefined {
  return storage.getStore();
}
