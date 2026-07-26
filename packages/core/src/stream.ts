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

/**
 * One administratively-created subscription (Orleans `Orleans.Streams.Core.StreamSubscription`):
 * an arbitrary grain reference bound to a stream from outside that grain,
 * without it ever calling `subscribe()` itself.
 */
export interface StreamSubscription {
  readonly subscriptionId: string;
  readonly streamId: StreamId;
  readonly grainId: GrainId;
}

/**
 * Administrative subscription management for a provider running in explicit
 * pub-sub mode (Orleans `IStreamSubscriptionManager`, retrieved via
 * `IStreamSubscriptionManagerAdmin.GetStreamSubscriptionManager(ExplicitSubscribeOnly)`):
 * attach/detach/enumerate a stream's subscribers by grain id, independent of
 * any implicit-subscription declaration or the grain calling `subscribe()`
 * itself. `streamId.provider` is ignored by callers of a provider-scoped
 * manager (it is implied by which provider's manager is being used) but kept
 * on `StreamId` for symmetry with `AsyncStream.id`.
 */
export interface StreamSubscriptionManager {
  addSubscription(streamId: StreamId, grainId: GrainId): Promise<StreamSubscription>;
  removeSubscription(streamId: StreamId, subscriptionId: string): Promise<void>;
  getSubscriptions(streamId: StreamId): Promise<StreamSubscription[]>;
}

/**
 * A server-side predicate that suppresses delivery of a published item before
 * it reaches any consumer (Orleans `IStreamFilter.ShouldDeliver`, wired via
 * `ISiloBuilder.AddStreamFilter`). Returning `false` drops the item entirely
 * for every subscriber on that stream — it never reaches a handler, and no
 * delivery is observed for it. `filterData` mirrors Orleans' per-subscription
 * filter payload (Orleans `IStreamSubscriptionManager`'s filter data); most
 * filters ignore it.
 */
export interface StreamFilter {
  shouldDeliver(streamId: StreamId, item: unknown, filterData?: string): boolean;
}

/**
 * A live, explicit producer registration on a stream (Orleans' pub-sub
 * `RegisterProducer` side, mirrored here for administrative visibility — e.g.
 * an operator or test inspecting which grains are currently producing to a
 * given stream). Registration is purely bookkeeping: `AsyncStream.publish`
 * works identically whether or not a producer ever registers one of these.
 */
export interface StreamProducerHandle {
  readonly streamId: StreamId;
  unregister(): Promise<void>;
}

export interface StreamProvider {
  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T>;
  /**
   * Present only on a provider running in explicit pub-sub mode (Orleans
   * `IStreamProvider.TryGetStreamSubscriptionManager`); absent on a provider
   * that only supports a grain subscribing itself or declaring
   * `@implicitStreamSubscription`.
   */
  getStreamSubscriptionManager?(): StreamSubscriptionManager | undefined;
  /**
   * Explicitly register as a producer for a stream (Orleans' pub-sub
   * `RegisterProducer`), returning a handle to `unregister()` when done
   * producing. Optional: a provider that has no notion of tracked producers
   * (or that registers them implicitly on first `getStream`/`publish`) can
   * omit it.
   */
  registerProducer?(namespace: string, key: GrainKey): Promise<StreamProducerHandle>;
}

/** Orleans `IStreamProvider.TryGetStreamSubscriptionManager(out manager)`, as a lookup. */
export function tryGetStreamSubscriptionManager(
  provider: StreamProvider,
): StreamSubscriptionManager | undefined {
  return provider.getStreamSubscriptionManager?.();
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
 *
 * The SAME symbol also serves an administratively-attached subscriber (see
 * `StreamSubscriptionManager.addSubscription`), where it plays the role of
 * Orleans' `IStreamSubscriptionObserver.OnSubscribed(handleFactory)`: called
 * once, the first time delivery is attempted, to decide whether the grain
 * wants the subscription at all. Returning `undefined` there declines it
 * (Orleans: `handle.UnsubscribeAsync()` without ever resuming) — the runtime
 * then drops the subscription. A grain that never declared this symbol at all
 * (the common implicit case) is not affected: absence, not a declined return,
 * is what silently drops delivery.
 */
export const STREAM_SUBSCRIPTION_OBSERVER = Symbol.for("thresh.streamSubscriptionObserver");

/** A grain that observes its implicitly- or administratively-subscribed streams. */
export interface ImplicitStreamSubscriber {
  [STREAM_SUBSCRIPTION_OBSERVER](
    namespace: string,
    key: string,
  ): StreamHandler<unknown> | undefined;
}

/** The grain's implicit-stream observer, bound to it, or `undefined` if it declares none. */
export function implicitStreamObserver(
  instance: object,
): ((namespace: string, key: string) => StreamHandler<unknown> | undefined) | undefined {
  return createSymbolObserver<StreamHandler<unknown> | undefined>(
    instance,
    STREAM_SUBSCRIPTION_OBSERVER,
  );
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

/**
 * A component (typically a stream provider) that accepts an opaque,
 * administratively-issued control command and returns its result (Orleans
 * `IControllable.ExecuteCommand`, reached via
 * `IManagementGrain.SendControlCommandToProvider`). The command's meaning and
 * argument shape are private to the implementation — the caller and callee
 * agree on both out of band.
 */
export interface Controllable {
  /** Orleans `IControllable.ExecuteCommand`: run an opaque control command, return its result. */
  executeCommand(command: number, arg: unknown): Promise<unknown>;
}

/**
 * Orleans `StreamGeneratorCommand.Configure`: the control command a
 * generator stream provider's `IControllable.ExecuteCommand` understands,
 * reconfiguring its generator live (see `GeneratorPullingStreamProvider.reconfigure`).
 */
export const STREAM_GENERATOR_COMMAND_CONFIGURE = 20000;
