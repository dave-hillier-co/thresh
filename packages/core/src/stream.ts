import type { GrainId } from "./grain-id";
import { defineGrainInterface, type GrainInterface } from "./grain-interface";
import type { GrainKey } from "./grain-key";
import { createSymbolObserver } from "./symbol-observer";

/** Identifies one stream: `(provider, namespace, key)`. */
export interface StreamId {
  provider: string;
  namespace: string;
  key: string;
}

/** A consumer's position in a stream; lets it resume exactly where it left off. */
export class SequenceToken {
  constructor(readonly value: number) {}
}

/** One item within a delivered batch, paired with its position (Orleans `SequentialItem<T>`). */
export interface BatchedStreamItem<T> {
  event: T;
  token: SequenceToken;
}

export interface StreamHandler<T> {
  onNext(event: T, token: SequenceToken): Promise<void>;
  onError?(err: unknown): Promise<void>;
  onCompleted?(): Promise<void>;
  /**
   * Receive several consecutive events in one call (Orleans `IAsyncBatchObserver.OnNextBatchAsync`
   * consumer side). Optional: a handler that omits it always receives events one at a time via
   * `onNext`, even when the producer published them as a batch — a provider that supports batched
   * delivery (see `AsyncStream.publishBatch`) only groups a delivery when the subscribed handler
   * declares this.
   */
  onNextBatch?(items: readonly BatchedStreamItem<T>[]): Promise<void>;
}

export interface StreamSubscriptionHandle<T> {
  /** Re-attach a handler after reactivation, resuming from the saved cursor. */
  resume(handler: StreamHandler<T>): Promise<void>;
  unsubscribe(): Promise<void>;
}

export interface SubscribeOptions {
  /** Rewind to a position the backing store still retains. */
  startToken?: SequenceToken;
  /**
   * Identifies the subscribing consumer. The runtime binds this to the calling
   * grain's activation; it scopes `getSubscriptions` so that, when many consumers
   * share one stream, each reacquires only its own durable subscription.
   */
  consumerId?: string;
}

/** A managed event stream addressed by identity; producers publish, consumers subscribe. */
export interface AsyncStream<T> {
  readonly id: StreamId;
  publish(event: T): Promise<void>;
  /**
   * Publish several events as one batch (Orleans `IAsyncStream.OnNextBatchAsync`). Optional: a
   * provider that doesn't support batched publish can omit it — a caller falls back to one
   * `publish` per event. A subscribed handler still receives the events one at a time via `onNext`
   * unless it declares `onNextBatch` (see `StreamHandler`).
   */
  publishBatch?(events: readonly T[]): Promise<void>;
  subscribe(
    handler: StreamHandler<T>,
    options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>>;
  /**
   * Re-bind existing durable subscriptions after reactivation. When `consumerId`
   * is given, only that consumer's subscriptions are returned (others sharing the
   * stream are excluded); omitting it returns every subscription on the stream.
   */
  getSubscriptions(consumerId?: string): Promise<StreamSubscriptionHandle<T>[]>;
}

export interface StreamProvider {
  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T>;
}

/**
 * A grain implicitly subscribed to one or more stream namespaces (declared with
 * `@implicitStreamSubscription` / `defineGrain`'s `implicitSubscriptions`)
 * exposes, under this symbol, the handler that receives a matching stream's
 * events. The runtime calls it lazily the first time an event for a given stream
 * arrives — mirroring Orleans' `IStreamSubscriptionObserver.OnSubscribed`, the
 * grain never calls `subscribe()`. The symbol key keeps it from colliding with
 * the grain's own (string-named) methods. Orleans' `StreamId` is namespace+key
 * (the provider is implied by context), so that is what the observer is given.
 */
export const STREAM_SUBSCRIPTION_OBSERVER = Symbol.for("tsva.streamSubscriptionObserver");

/** A grain that observes its implicitly-subscribed streams. */
export interface ImplicitStreamSubscriber {
  [STREAM_SUBSCRIPTION_OBSERVER](namespace: string, key: string): StreamHandler<unknown>;
}

/** The grain's implicit-stream observer, bound to it, or `undefined` if it declares none. */
export function implicitStreamObserver(
  instance: object,
): ((namespace: string, key: string) => StreamHandler<unknown>) | undefined {
  return createSymbolObserver<StreamHandler<unknown>>(instance, STREAM_SUBSCRIPTION_OBSERVER);
}

/**
 * System extension a pulling agent invokes (through the dispatcher) to hand an
 * event to a subscriber grain's single activation. The runtime intercepts it and
 * runs the activation's registered handler as a turn — the grain never declares
 * this method. Mirrors the reminder delivery path (`Remindable`).
 */
export interface StreamConsumer {
  deliverStreamEvent(streamKey: string, event: unknown, token: number): Promise<void>;
}

export const StreamConsumerInterface: GrainInterface<StreamConsumer> =
  defineGrainInterface<StreamConsumer>("system.StreamConsumer");

/**
 * Lets the runtime register a subscribing grain's handler on its activation so a
 * pulling-agent delivery (which arrives as a `StreamConsumer` turn) reaches it.
 * The memory provider does not need this — it wraps `onNext` as a turn directly.
 */
export interface StreamActivationBinding {
  readonly grainId: GrainId;
  setHandler(streamKey: string, handler: StreamHandler<unknown>): void;
  clearHandler(streamKey: string): void;
}

/**
 * A silo-level stream provider whose delivery is driven by pulling agents. The
 * runtime calls `bindActivation` so each subscribing grain registers its handler
 * on its own activation; the agent then routes events there through the
 * dispatcher. `MemoryStreamProvider` is a plain `StreamProvider` (no binding).
 */
export interface ActivationBoundStreamProvider extends StreamProvider {
  bindActivation(binding: StreamActivationBinding): StreamProvider;
}

export function isActivationBound(
  provider: StreamProvider,
): provider is ActivationBoundStreamProvider {
  return typeof (provider as ActivationBoundStreamProvider).bindActivation === "function";
}
