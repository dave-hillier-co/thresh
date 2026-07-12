import type { ChannelId } from "./broadcast-channel";

/**
 * A delivery-lifecycle event `ClusterNode.publishToBroadcastChannel` emits
 * (Orleans `Orleans.BroadcastChannel.Diagnostics.BroadcastChannelEvents`,
 * consumed by `BroadcastChannelDiagnosticObserver` in
 * `test/TestInfrastructure/TestExtensions/Diagnostics`). Tests subscribe to
 * synchronize on "N deliveries have happened" without racing a `publish()`
 * call whose fan-out is not otherwise observable from outside the cluster —
 * this is the only reason these events exist; nothing in the runtime itself
 * consumes them.
 */
export type BroadcastChannelEvent =
  | { kind: "itemPublished"; providerName: string; channelId: ChannelId }
  | { kind: "itemDelivered"; providerName: string; channelId: ChannelId };

export type BroadcastChannelEventListener = (event: BroadcastChannelEvent) => void;

const listeners = new Set<BroadcastChannelEventListener>();

/** Subscribe to every broadcast-channel diagnostic event; returns an unsubscribe function. */
export function onBroadcastChannelEvent(listener: BroadcastChannelEventListener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** Emit a diagnostic event to every current subscriber (`ClusterNode` calls this). */
export function emitBroadcastChannelEvent(event: BroadcastChannelEvent): void {
  for (const listener of listeners) listener(event);
}
