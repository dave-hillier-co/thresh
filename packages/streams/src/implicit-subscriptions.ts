import { GrainId } from "@tsva/core/grain-id";
import type { GrainType } from "@tsva/core/grain-type";

/**
 * Resolve a stream's implicit subscribers (Orleans' `[ImplicitStreamSubscription]`).
 * A stream key is `namespace/key`; `typesFor` returns the grain types implicitly
 * subscribed to the namespace. Each such type with the stream's key is a
 * subscriber, so the synthesized `GrainId` is `(type, key)` — the grain whose
 * primary key matches the stream is auto-subscribed (a pulling agent delivers to
 * it without any explicit `subscribe`).
 *
 * The key is taken as a string: stream keys are string-encoded on the wire, and
 * implicit subscribers are addressed by that key (matching how a producer names
 * the stream). Integer/Guid-keyed implicit subscribers are out of scope.
 */
export function implicitSubscriberIds(
  streamKey: string,
  typesFor: (namespace: string) => Iterable<GrainType>,
): GrainId[] {
  const slash = streamKey.indexOf("/");
  if (slash < 0) return [];
  const namespace = streamKey.slice(0, slash);
  const key = streamKey.slice(slash + 1);
  return [...typesFor(namespace)].map((type) => new GrainId(type, key));
}
