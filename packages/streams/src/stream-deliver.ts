import type { GrainId } from "@thresh/core/grain-id";

/**
 * Delivers one stream event to a subscriber's activation; wired to
 * `ClusterNode.deliverStreamEvent` by the hosting layer (`SiloBuilder.build()`).
 * Shared by every stream provider that needs to fan an event out to a grain's
 * activation through the dispatcher (rather than a direct in-process callback) —
 * currently the Redis pulling provider (explicit + implicit subscribers) and the
 * memory provider (implicit subscribers only; explicit ones are handled by a
 * direct handler callback, see `memory-stream-provider.ts`).
 */
export type StreamDeliver = (
  subscriber: GrainId,
  streamKey: string,
  event: unknown,
  token: number,
) => Promise<void>;
