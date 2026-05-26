import { defineGrainInterface, type GrainInterface } from "./grain-interface";

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
  const fn = (instance as Record<symbol, unknown>)[BROADCAST_CHANNEL_OBSERVER];
  return typeof fn === "function"
    ? (namespace, key) =>
        (fn as OnBroadcastChannelSubscribed[typeof BROADCAST_CHANNEL_OBSERVER]).call(
          instance,
          namespace,
          key,
        )
    : undefined;
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
