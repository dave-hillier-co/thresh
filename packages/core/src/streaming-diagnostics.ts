import type { StreamId } from "./stream";

/**
 * A delivery-lifecycle event a stream provider emits (Orleans
 * `Orleans.Streaming.Diagnostics.StreamingEvents`, consumed by
 * `StreamingDiagnosticObserver` in `test/TestInfrastructure/TestExtensions/Diagnostics`).
 * Tests subscribe to synchronize on "N items have been delivered" or "this
 * stream has gone inactive" without racing a `publish()` call (or a timer)
 * whose effect is not otherwise observable from outside the cluster — this is
 * the only reason these events exist; nothing in the runtime itself consumes
 * them.
 */
export type StreamingEvent =
  | { kind: "itemDelivered"; providerName: string; streamId: StreamId }
  | { kind: "streamInactive"; providerName: string; streamId: StreamId };

export type StreamingEventListener = (event: StreamingEvent) => void;

const listeners = new Set<StreamingEventListener>();

/** Subscribe to every streaming diagnostic event; returns an unsubscribe function. */
export function onStreamingEvent(listener: StreamingEventListener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** Emit a diagnostic event to every current subscriber (a stream provider calls this). */
export function emitStreamingEvent(event: StreamingEvent): void {
  for (const listener of listeners) listener(event);
}
