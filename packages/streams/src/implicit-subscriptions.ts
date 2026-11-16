import { GrainId } from "@thresh/core/grain-id";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import type { GrainType } from "@thresh/core/grain-type";

/**
 * Resolve a stream's implicit subscribers (Orleans' `[ImplicitStreamSubscription]`).
 * A stream key is `namespace/key`; `typesFor` returns the grain types implicitly
 * subscribed to the namespace. Each such type with the stream's key is a
 * subscriber: the grain whose primary key matches the stream is auto-subscribed
 * (a pulling agent delivers to it without any explicit `subscribe`).
 *
 * Stream keys are string-encoded on the wire, so the key arrives as text. As in
 * Orleans (`DefaultStreamIdMapper` reads the key through the grain type's bindings),
 * the grain type decides how that text is read: `keyKindFor` gives the kind a type
 * declares, and the id is built through it, so an integer-keyed grain is addressed
 * by an integer key. A type that declares none is addressed by the text itself.
 * Identity is type and key text either way, so both name the same activation.
 */
export function implicitSubscriberIds(
  streamKey: string,
  typesFor: (namespace: string) => Iterable<GrainType>,
  keyKindFor?: (type: GrainType) => GrainKeyKind | undefined,
): GrainId[] {
  const slash = streamKey.indexOf("/");
  if (slash < 0) return [];
  const namespace = streamKey.slice(0, slash);
  const key = streamKey.slice(slash + 1);
  return [...typesFor(namespace)].map((type) => {
    const id = new GrainId(type, key);
    const kind = keyKindFor?.(type);
    return kind === undefined ? id : id.asKind(kind);
  });
}
