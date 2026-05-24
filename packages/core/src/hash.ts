/**
 * FNV-1a 32-bit hash followed by a MurmurHash3 finalizer. Stable across
 * processes so every silo derives the same ring positions and interface ids
 * from the same inputs. The finalizer gives good avalanche, which the directory
 * ring relies on to distribute similar keys ("silo-0#1", "Counter/g-2") evenly.
 */
export function stableHash32(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  // MurmurHash3 fmix32 finalizer.
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}
