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

export interface GrainCancellationTokenInit {
  tokenId: string;
  signal: AbortSignal;
  /** Caller-side only: the source that minted this token, for `recordTarget`. */
  source?: GrainCancellationTokenSource;
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
    await Promise.all(
      [...this.targets.values()].map((target) => this.canceller(target, this._tokenId)),
    );
  }
}
