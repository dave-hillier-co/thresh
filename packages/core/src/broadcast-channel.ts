import { GrainId } from "./grain-id";
import { defineGrainInterface, type GrainInterface } from "./grain-interface";
import { createSymbolObserver } from "./symbol-observer";

/**
 * Identifies one broadcast channel: `(namespace, key)`. Unlike a `StreamId` there
 * is no provider field — the provider is the writer you obtained it from. Orleans'
 * `ChannelId` is a namespace + key pair (`Orleans.BroadcastChannel/ChannelId.cs`).
 */
export interface ChannelId {
  namespace: string;
  key: string;
}

/** The wire form of a `ChannelId`, `namespace/key` — mirrors a stream key. */
export function channelKey(id: ChannelId): string {
  return `${id.namespace}/${id.key}`;
}

/**
 * A consumer's per-channel callbacks. `onPublished` runs as a turn on the
 * subscriber grain's activation; `onError` (optional) is informational. Mirrors
 * the `Func<T, Task>`/`Func<Exception, Task>` pair a grain registers via
 * `IBroadcastChannelSubscription.Attach` (`BroadcastChannelSubscription.cs`).
 */
export interface BroadcastChannelHandler<T> {
  onPublished(item: T): Promise<void> | void;
  onError?(err: unknown): Promise<void> | void;
}

/**
 * A grain implicitly subscribed to one or more channel namespaces (declared with
 * `@implicitChannelSubscription` / `defineGrain`'s `implicitChannelSubscriptions`)
 * exposes, under this symbol, the handler that receives a matching channel's
 * items. The runtime calls it lazily the first time an item for a given channel
 * arrives — mirroring Orleans' `IOnBroadcastChannelSubscribed.OnSubscribed`
 * (`BroadcastChannelSubscription.cs`), the grain never calls `subscribe()`. The
 * symbol key keeps it from colliding with the grain's own (string-named) methods.
 */
export const BROADCAST_CHANNEL_OBSERVER = Symbol.for("tsva.broadcastChannelObserver");

/** A grain that observes its implicitly-subscribed broadcast channels. */
export interface OnBroadcastChannelSubscribed {
  [BROADCAST_CHANNEL_OBSERVER](namespace: string, key: string): BroadcastChannelHandler<unknown>;
}

/** The grain's broadcast observer, bound to it, or `undefined` if it declares none. */
export function broadcastChannelObserver(
  instance: object,
): ((namespace: string, key: string) => BroadcastChannelHandler<unknown>) | undefined {
  return createSymbolObserver<BroadcastChannelHandler<unknown>>(
    instance,
    BROADCAST_CHANNEL_OBSERVER,
  );
}

/**
 * System extension the writer invokes (through the dispatcher) to hand a
 * published item to a subscriber grain's single activation. The runtime
 * intercepts it and runs the grain's broadcast observer as a turn — the grain
 * never declares this method. Mirrors the `StreamConsumer` delivery path.
 */
export interface BroadcastConsumer {
  onPublished(channelKey: string, item: unknown): Promise<void>;
}

export const BroadcastConsumerInterface: GrainInterface<BroadcastConsumer> =
  defineGrainInterface<BroadcastConsumer>("system.BroadcastConsumer");

/** Publishes items to a single channel (Orleans `IBroadcastChannelWriter<T>`). */
export interface BroadcastChannelWriter<T> {
  /** Publish an item to every grain implicitly subscribed to the channel's namespace. */
  publish(item: T): Promise<void>;
}

/** Hands out writers for channels (Orleans `IBroadcastChannelProvider`). */
export interface BroadcastChannelProvider {
  getChannelWriter<T>(channel: ChannelId): BroadcastChannelWriter<T>;
}

/**
 * Per-named-provider broadcast-channel config (Orleans `BroadcastChannelOptions`).
 * `fireAndForgetDelivery` decides `ClusterNode.publishToBroadcastChannel`'s
 * failure semantics: fire-and-forget swallows a subscriber's throw (visible
 * only via delivery-count diagnostics), while non-fire-and-forget awaits
 * every subscriber and, once all have been tried, throws an `AggregateError`
 * collecting every failure (Orleans' `AggregateException`). Defaults to
 * `false` (non-fire-and-forget) here — UNLIKE Orleans, whose default is
 * `true` — since this framework's broadcast channels predate the option and
 * always awaited every subscriber for error visibility; opt into Orleans'
 * default per provider with `{ fireAndForgetDelivery: true }`.
 */
export interface BroadcastChannelOptions {
  fireAndForgetDelivery?: boolean;
}

/**
 * System extension a client (outside any grain) invokes to publish onto a
 * broadcast channel through its gateway silo (Orleans `IClusterClient.
 * GetBroadcastChannelProvider`). Unlike `BroadcastConsumerInterface`, this
 * doesn't address a grain activation at all — `ClusterNode.receiveRequest`
 * intercepts a call to this interface before placement/directory routing and
 * calls `publishToBroadcastChannel` directly on the receiving (gateway) silo,
 * which then fans the item out cluster-wide exactly like a grain-originated
 * publish.
 */
export interface BroadcastChannelPublisher {
  publish(providerName: string, channel: ChannelId, item: unknown): Promise<void>;
}

export const BroadcastChannelPublisherInterface: GrainInterface<BroadcastChannelPublisher> =
  defineGrainInterface<BroadcastChannelPublisher>("system.BroadcastChannelPublisher", {
    extension: true,
  });

/**
 * The fixed pseudo-target a client's `BroadcastChannelProvider` addresses a
 * publish call to — never a real grain activation (see
 * `BroadcastChannelPublisherInterface`), so any stable `GrainId` works; one
 * reserved constant keeps every client's publish call routable the same way.
 */
export function broadcastPublisherGrainId(): GrainId {
  return new GrainId("$system.broadcastPublisher", "publisher");
}
