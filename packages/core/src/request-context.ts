import { AsyncLocalStorage } from "node:async_hooks";
import { Guid } from "./guid";

/**
 * Orleans `RequestContext`: an ambient string→string bag that rides a call
 * chain — W3C trace context, tenant/correlation ids, and other app baggage a
 * caller wants a callee to see without threading it through every method
 * signature. Backed by a single {@link AsyncLocalStorage}, shared by both a
 * grain turn (`@thresh/runtime`'s `requestContext` delegates here, scoping a
 * fresh store per turn via {@link runWithRequestContext}) and a non-grain
 * caller (a client / test, calling {@link RequestContext.set} directly,
 * below) — mirroring .NET's `RequestContext`, which is the same static class
 * whether called from grain code or client code.
 */
const storage = new AsyncLocalStorage<Record<string, string>>();

/**
 * Orleans `IPlacementDirector.PlacementHintKey`: the well-known `RequestContext`
 * key a caller sets (`RequestContext.set(PLACEMENT_HINT_KEY, silo.toString())`)
 * to steer a grain's activation onto a chosen silo. Honoured by the placement
 * decision when the hinted silo is a live candidate; otherwise the normal
 * placement strategy runs (see `@thresh/runtime/placement/placement-hint`).
 */
export const PLACEMENT_HINT_KEY = "PlacementHintKey";

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
  /**
   * The call-chain reentrancy id ambient here, if any (Orleans
   * `RequestContext.ReentrancyId`, `Guid.Empty` there). Set by
   * {@link RequestContext.allowCallChainReentrancy} and carried by every grain
   * call made in its scope, and onward down the chain those calls start.
   */
  get reentrancyId(): string | undefined {
    return callChainStorage.getStore()?.reentrancyId;
  },
  /**
   * Orleans `RequestContext.AllowCallChainReentrancy()`: run `fn` with
   * call-chain reentrancy allowed for the grain calls it makes. Call-chain
   * reentrancy is OFF by default, so without this a non-reentrant grain that
   * calls back into itself — directly, or through other grains — deadlocks
   * until the call's response timeout. Inside `fn`, the current activation
   * (when called from a grain turn) admits any request carrying this chain's
   * id while it is busy, so `A -> B -> A` and `A -> A` complete. Only the
   * grain that opted in is re-enterable: `A -> B -> A -> B` still deadlocks on
   * `B` unless `B` opted in too.
   *
   * The scope is the callback's async extent, not a disposable as in C#
   * (`using var _ = RequestContext.AllowCallChainReentrancy();`): the section
   * stays open until `fn`'s promise settles, and concurrent branches of the
   * same turn outside `fn` are unaffected. An id already ambient is reused, as
   * in Orleans; otherwise a fresh one is minted.
   */
  async allowCallChainReentrancy<T>(fn: () => Promise<T>): Promise<T> {
    const current = callChainStorage.getStore();
    const reentrancyId = current?.reentrancyId ?? Guid.newGuid().toString();
    const grainContext = current?.grainContext;
    grainContext?.onEnterReentrantSection(reentrancyId);
    try {
      return await callChainStorage.run({ reentrancyId, grainContext }, fn);
    } finally {
      grainContext?.onExitReentrantSection(reentrancyId);
    }
  },
  /**
   * Orleans `RequestContext.SuppressCallChainReentrancy()`: run `fn` with no
   * call-chain reentrancy id ambient, so the grain calls it makes (and the
   * chains they start) are not admitted by any grain's open reentrant
   * section. Used where a call must not ride the caller's chain — Orleans'
   * stream consumer suppresses it around its subscription calls.
   */
  suppressCallChainReentrancy<T>(fn: () => Promise<T>): Promise<T> {
    const grainContext = callChainStorage.getStore()?.grainContext;
    return callChainStorage.run({ reentrancyId: undefined, grainContext }, fn);
  },
};

/**
 * Orleans `ICallChainReentrantGrainContext`: the activation a reentrant
 * section is opened on. While a section for an id is open, the activation
 * admits a request carrying that id even though another turn is running.
 * Implemented by the runtime's activation; grain code never sees it.
 */
export interface CallChainReentrantGrainContext {
  onEnterReentrantSection(reentrancyId: string): void;
  onExitReentrantSection(reentrancyId: string): void;
}

interface CallChainScope {
  readonly reentrancyId: string | undefined;
  readonly grainContext: CallChainReentrantGrainContext | undefined;
}

/**
 * The call-chain half of Orleans' `RequestContext`: the reentrancy id (its
 * `#CCR` entry) and the activation a section would open on (Orleans'
 * `RuntimeContext.Current`). Kept out of the string headers bag so it never
 * shows in {@link RequestContext.keys} and so a section's scope is its
 * callback, not a mutation of the turn's shared bag.
 */
const callChainStorage = new AsyncLocalStorage<CallChainScope>();

/**
 * Run `fn` as a turn of `grainContext`, with `reentrancyId` (the incoming
 * request's, or none) ambient — used by the runtime to scope each grain turn,
 * so the id propagates to the calls the turn makes and a section the turn
 * opens lands on its own activation.
 */
export function runWithCallChain<T>(
  reentrancyId: string | undefined,
  grainContext: CallChainReentrantGrainContext | undefined,
  fn: () => T,
): T {
  return callChainStorage.run({ reentrancyId, grainContext }, fn);
}

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
