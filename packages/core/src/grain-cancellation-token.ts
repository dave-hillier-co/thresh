import { GrainTaskCanceledError } from "./errors";
import type { GrainId } from "./grain-id";
import { Guid } from "./guid";

/**
 * Callee-side placeholder produced by `decodeValue` for a wire-arrived
 * `GrainCancellationToken` argument. `decodeValue` has no activation context
 * (see `CodecContext`), so it cannot bind a live `AbortSignal` — that happens
 * later, in `ActivationData.callMethod`, which replaces each placeholder with
 * a real `GrainCancellationToken` bound to this activation's cancellation
 * extension before the grain method runs.
 */
export class CancellationTokenPlaceholder {
  constructor(
    readonly tokenId: string,
    readonly cancelled: boolean,
  ) {}
}

/**
 * Non-enumerable, module-internal back-reference from a caller-side
 * `GrainCancellationToken` to the `GrainCancellationTokenSource` that minted
 * it. Only a token returned by `GrainCancellationTokenSource.token` carries
 * one; a callee-side token (bound from a `CancellationTokenPlaceholder` by
 * the activation) has none. Exposed via `cancellationTokenSourceOf` below —
 * the same "hidden symbol field + accessor" shape `grain-reference.ts` uses
 * for `GRAIN_REF`.
 */
const SOURCE: unique symbol = Symbol("tsva.cancellationTokenSource");

/**
 * Non-enumerable, module-internal back-reference from a callee-side
 * `GrainCancellationToken` (bound by `ActivationData.bindCancellationTokens`)
 * to the callback that records a grain this token was forwarded to onto the
 * binding activation's own `CancellationSourcesExtension`. Mutually exclusive
 * with `SOURCE` in practice: a token either was minted by a source
 * (caller-side) or bound from the wire with this hook (callee-side).
 */
const ON_DISPATCH: unique symbol = Symbol("tsva.cancellationTokenOnDispatch");

/**
 * Per-`AbortSignal` sink of exceptions thrown by `register` callbacks when
 * the signal fires. A callback registered via `signal.addEventListener`
 * cannot let its exception escape the dispatch: Node re-throws a listener's
 * exception as an *uncaught* error on a later tick (`process.nextTick`),
 * which would crash the worker rather than surface anywhere catchable — so
 * `register` wraps each callback, captures any throw here, and the code that
 * fired the cancellation (`CancellationSourcesExtension.cancelRemoteToken`)
 * drains the sink to decide whether to propagate. Mirrors Orleans, where a
 * `GrainCancellationToken` callback's exception flows back to the caller of
 * `GrainCancellationTokenSource.Cancel()`.
 */
const CALLBACK_ERRORS: unique symbol = Symbol("tsva.cancellationCallbackErrors");

type ErrorSink = { [CALLBACK_ERRORS]?: unknown[] };

function pushCallbackError(signal: AbortSignal, error: unknown): void {
  const holder = signal as unknown as ErrorSink;
  (holder[CALLBACK_ERRORS] ??= []).push(error);
}

/**
 * Take and clear the exceptions thrown by `register` callbacks that fired on
 * `signal`. Returns `[]` when no callback threw (the common case — no
 * behaviour change for callers that never registered a throwing callback).
 */
export function drainCancellationCallbackErrors(signal: AbortSignal): unknown[] {
  const holder = signal as unknown as ErrorSink;
  const errors = holder[CALLBACK_ERRORS];
  if (errors === undefined) return [];
  delete holder[CALLBACK_ERRORS];
  return errors;
}

export interface GrainCancellationTokenInit {
  tokenId: string;
  signal: AbortSignal;
  /** Caller-side only: the source that minted this token, for `recordTarget`. */
  source?: GrainCancellationTokenSource;
  /**
   * Callee-side only: called when THIS token is forwarded as an argument to
   * another grain call, so the activation that bound this token (via
   * `bindCancellationTokens`) can record the forwarded target on its own
   * `CancellationSourcesExtension` — the hop that makes cascading
   * cancellation possible (see `recordCancellationTarget`).
   */
  onDispatchToTarget?: (target: GrainId) => void;
}

/**
 * Cooperative cancellation token (Orleans `GrainCancellationToken`): JS has no
 * thread interruption, so cancellation only ever *signals* — the callee must
 * observe `signal`/`isCancellationRequested` and stop itself. Backed by a
 * plain `AbortSignal` so callee code can use it directly (`signal.aborted`,
 * `signal.addEventListener("abort", ...)`) alongside the token's own helpers.
 */
export class GrainCancellationToken {
  private readonly _tokenId: string;
  private readonly _signal: AbortSignal;

  constructor(init: GrainCancellationTokenInit) {
    this._tokenId = init.tokenId;
    this._signal = init.signal;
    if (init.source !== undefined) {
      Object.defineProperty(this, SOURCE, { value: init.source, enumerable: false });
    }
    if (init.onDispatchToTarget !== undefined) {
      Object.defineProperty(this, ON_DISPATCH, {
        value: init.onDispatchToTarget,
        enumerable: false,
      });
    }
  }

  get tokenId(): string {
    return this._tokenId;
  }

  get signal(): AbortSignal {
    return this._signal;
  }

  get isCancellationRequested(): boolean {
    return this._signal.aborted;
  }

  /** Throw `GrainTaskCanceledError` if this token's signal has fired. */
  throwIfCancellationRequested(): void {
    if (this._signal.aborted) throw new GrainTaskCanceledError();
  }

  /**
   * Register `callback` to run when this token is cancelled (Orleans
   * `GrainCancellationToken.CancellationToken.Register`). If the token has
   * already fired, `callback` runs synchronously right now and any exception
   * it throws propagates to this caller, mirroring `.NET`'s
   * `CancellationToken.Register` on an already-cancelled token. Otherwise it
   * fires once, on the underlying `AbortSignal`'s `abort` event; the returned
   * handle's `dispose()` removes the listener so a completed call can stop
   * observing.
   *
   * A callback that throws when fired via the signal cannot let its exception
   * escape the event dispatch (Node would turn it into an uncaught error and
   * crash the worker), so the throw is captured into a per-signal sink
   * (`drainCancellationCallbackErrors`); the code that fired the cancellation
   * decides whether to propagate it.
   */
  register(callback: () => void): { dispose(): void } {
    if (this._signal.aborted) {
      callback();
      return { dispose(): void {} };
    }
    const signal = this._signal;
    const listener = (): void => {
      try {
        callback();
      } catch (error) {
        pushCallbackError(signal, error);
      }
    };
    signal.addEventListener("abort", listener, { once: true });
    return {
      dispose(): void {
        signal.removeEventListener("abort", listener);
      },
    };
  }
}

/**
 * The `GrainCancellationTokenSource` that minted `token`, if any (a
 * callee-side token bound from the wire has none). Used by the grain-factory
 * dispatch hook to record which target grains a token was sent to, so
 * `source.cancel()` knows who to notify.
 */
export function cancellationTokenSourceOf(
  token: GrainCancellationToken,
): GrainCancellationTokenSource | undefined {
  return (token as unknown as Record<symbol, GrainCancellationTokenSource | undefined>)[SOURCE];
}

/**
 * Record that `token` was just dispatched to `target` — the single place the
 * grain-factory's outgoing-call hook calls, covering BOTH shapes a
 * `GrainCancellationToken` can be in on the sending side:
 *
 * - Caller-side (minted by a `GrainCancellationTokenSource`, still carries its
 *   `source` back-reference): records `target` on the source, so a later
 *   `source.cancel()` notifies it directly.
 * - Callee-side (rebuilt from a wire `CancellationTokenPlaceholder`, no
 *   source, but bound with an `onDispatchToTarget` hook by the activation
 *   that owns it): records `target` as a FORWARDED target on that
 *   activation's own `CancellationSourcesExtension`, so a `cancelRemoteToken`
 *   reaching this activation cascades on to `target` too — the mechanism that
 *   lets a client-originated cancellation reach a second grain hop.
 *
 * A token with neither (freshly bound, never forwarded) is a no-op here.
 */
export function recordCancellationTarget(token: GrainCancellationToken, target: GrainId): void {
  const source = cancellationTokenSourceOf(token);
  if (source !== undefined) {
    source.recordTarget(target);
    return;
  }
  const onDispatch = (token as unknown as Record<symbol, ((target: GrainId) => void) | undefined>)[
    ON_DISPATCH
  ];
  onDispatch?.(target);
}

/**
 * Caller-side source of a `GrainCancellationToken` (Orleans
 * `GrainCancellationTokenSource`). `cancel()` aborts the local signal and
 * tells every recorded target grain to abort its bound copy too, via
 * `canceller` — cross-silo propagation rides the already-built
 * `IGrainExtension` substrate (`ICancellationSourcesExtension`,
 * `@tsva/runtime/cancellation-extension`).
 *
 * `canceller` is injected rather than reaching for an ambient runtime/factory
 * so this class stays a plain `@tsva/core` value type with no dependency on
 * `@tsva/runtime`. What the *public* API for constructing a
 * `GrainCancellationTokenSource` inside a grain looks like (e.g. wiring
 * `canceller` from `this.runtime` automatically) is left to the later parity
 * port — this task only builds the mechanism.
 */
export class GrainCancellationTokenSource {
  private readonly controller = new AbortController();
  private readonly _tokenId = Guid.newGuid().toString();
  /** Recorded call targets, keyed by `grainId.toString()` to dedupe. */
  private readonly targets = new Map<string, GrainId>();

  constructor(private readonly canceller: (target: GrainId, tokenId: string) => Promise<void>) {}

  get tokenId(): string {
    return this._tokenId;
  }

  /** A fresh token bound to this source's signal, carrying the back-reference `recordTarget` needs. */
  get token(): GrainCancellationToken {
    return new GrainCancellationToken({
      tokenId: this._tokenId,
      signal: this.controller.signal,
      source: this,
    });
  }

  /** Record a grain this token was just sent to, so `cancel()` knows to notify it. */
  recordTarget(target: GrainId): void {
    this.targets.set(target.toString(), target);
  }

  /**
   * Abort the local signal, then tell every recorded target to abort its
   * bound copy. Idempotent: aborting an already-aborted `AbortController` is
   * a no-op, and `cancelRemoteToken` on an already-cancelled remote extension
   * is harmless (aborting an already-aborted controller there too) — calling
   * `cancel()` again just re-sends the (now redundant) notifications.
   */
  async cancel(): Promise<void> {
    this.controller.abort();
    // Aborting fires callbacks registered on this source's own signal — the
    // in-process case, where the token reached the callee without a wire round
    // trip and so is still bound to this controller's signal (the cross-silo
    // case binds it to the remote activation's controller, drained by
    // `cancelRemoteToken`). A callback that threw was captured, not allowed to
    // escape the event dispatch; surface it so it propagates to this caller,
    // matching Orleans' "a GrainCancellationToken callback's exception flows to
    // Cancel()". Draining before notifying remote targets keeps ordering
    // simple; only one of the two sites ever holds an error for a given token.
    const localErrors = drainCancellationCallbackErrors(this.controller.signal);
    await Promise.all(
      [...this.targets.values()].map((target) => this.canceller(target, this._tokenId)),
    );
    const first = localErrors[0];
    if (first !== undefined) throw first;
  }
}
