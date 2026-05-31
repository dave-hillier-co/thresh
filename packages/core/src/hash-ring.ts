/** A half-open hash range `[begin, end)` a silo owns on the ring. */
export type HashRange = readonly [begin: number, end: number];

/**
 * Whether `hash` falls in the half-open range `[begin, end)`. Supports a range
 * that wraps the ring (`begin > end`), in which case membership is the union of
 * `[begin, max)` and `[0, end)`.
 */
export function isHashInRange(hash: number, begin: number, end: number): boolean {
  return begin <= end ? hash >= begin && hash < end : hash >= begin || hash < end;
}

/** Whether `hash` falls in any of the given half-open ranges. */
export function isHashInRanges(hash: number, ranges: readonly HashRange[]): boolean {
  return ranges.some(([begin, end]) => isHashInRange(hash, begin, end));
}
