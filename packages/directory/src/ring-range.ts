import type { HashRange } from "@thresh/core/hash-ring";

/** The largest point on the hash circle; points are unsigned 32-bit integers. */
export const RING_MAX = 0xffff_ffff;
const RING_SIZE = 0x1_0000_0000;

/** An inclusive run of points `[lo, hi]` on the circle laid out flat, `lo <= hi`. */
type Segment = readonly [lo: number, hi: number];

/**
 * A contiguous range of the hash circle, `(start, end]`: `start` exclusive, `end` inclusive
 * (Orleans `RingRange`). A virtual node at hash `h` whose predecessor is at `p` owns `(p, h]`,
 * which is what `ConsistentHashRing.owner` computes (the first vnode at or above a hash), so this
 * one definition decides the owner of a hash for the directory, reminders and streams alike.
 *
 * `empty` holds no point and `full` holds every one; an arc with `start === end` is empty.
 */
export class RingRange {
  static readonly empty = new RingRange(0, 0, "empty");
  static readonly full = new RingRange(0, 0, "full");

  private constructor(
    readonly start: number,
    readonly end: number,
    private readonly kind: "empty" | "full" | "arc",
  ) {}

  /** The arc `(start, end]`, wrapping past `RING_MAX` when `start > end`; empty when they are equal. */
  static of(start: number, end: number): RingRange {
    const s = start >>> 0;
    const e = end >>> 0;
    return s === e ? RingRange.empty : new RingRange(s, e, "arc");
  }

  /** The range holding exactly `point`. */
  static point(point: number): RingRange {
    return RingRange.of((point - 1) >>> 0, point);
  }

  get isEmpty(): boolean {
    return this.kind === "empty";
  }

  get isFull(): boolean {
    return this.kind === "full";
  }

  contains(point: number): boolean {
    if (this.kind === "empty") return false;
    if (this.kind === "full") return true;
    const p = point >>> 0;
    return this.start < this.end
      ? p > this.start && p <= this.end
      : p > this.start || p <= this.end;
  }

  intersects(other: RingRange): boolean {
    return intersectSegments(this.segments(), other.segments()).length > 0;
  }

  /** The points in both ranges, as at most two ranges. */
  intersection(other: RingRange): RingRange[] {
    return fromSegments(intersectSegments(this.segments(), other.segments()));
  }

  /** The points in this range and not in `other`, as at most two ranges. */
  difference(other: RingRange): RingRange[] {
    return fromSegments(subtractSegments(this.segments(), other.segments()));
  }

  equals(other: RingRange): boolean {
    return this.kind === other.kind && this.start === other.start && this.end === other.end;
  }

  /**
   * The same points as a half-open `[begin, end)` tuple, the shape reminder tables and stream
   * queue ownership take (`isHashInRange`). Not defined for an empty range.
   */
  toHashRange(): HashRange {
    if (this.kind === "full") return [0, RING_SIZE];
    if (this.kind === "empty") throw new Error("an empty ring range has no half-open form");
    return [(this.start + 1) % RING_SIZE, this.end + 1];
  }

  toString(): string {
    if (this.kind === "empty") return "(empty)";
    if (this.kind === "full") return "(full)";
    return `(0x${hex(this.start)}, 0x${hex(this.end)}]`;
  }

  /** This range laid flat on `[0, RING_MAX]`: one segment, or two when it wraps. */
  private segments(): Segment[] {
    if (this.kind === "empty") return [];
    if (this.kind === "full") return [[0, RING_MAX]];
    if (this.start < this.end) return [[this.start + 1, this.end]];
    const out: Segment[] = [];
    if (this.start < RING_MAX) out.push([this.start + 1, RING_MAX]);
    out.push([0, this.end]);
    return out;
  }

  /** @internal Segment view for the collection helpers below. */
  static segmentsOf(range: RingRange): Segment[] {
    return range.segments();
  }
}

function hex(n: number): string {
  return n.toString(16).padStart(8, "0");
}

function intersectSegments(a: readonly Segment[], b: readonly Segment[]): Segment[] {
  const out: Segment[] = [];
  for (const [alo, ahi] of a) {
    for (const [blo, bhi] of b) {
      const lo = Math.max(alo, blo);
      const hi = Math.min(ahi, bhi);
      if (lo <= hi) out.push([lo, hi]);
    }
  }
  return normalize(out);
}

function subtractSegments(a: readonly Segment[], b: readonly Segment[]): Segment[] {
  let remaining: Segment[] = [...a];
  for (const [blo, bhi] of b) {
    const next: Segment[] = [];
    for (const [lo, hi] of remaining) {
      if (bhi < lo || blo > hi) {
        next.push([lo, hi]);
        continue;
      }
      if (blo > lo) next.push([lo, blo - 1]);
      if (bhi < hi) next.push([bhi + 1, hi]);
    }
    remaining = next;
  }
  return normalize(remaining);
}

/** Sort and merge touching or overlapping segments. */
function normalize(segments: Segment[]): Segment[] {
  const sorted = [...segments].sort((x, y) => x[0] - y[0]);
  const out: Array<[number, number]> = [];
  for (const [lo, hi] of sorted) {
    const last = out[out.length - 1];
    if (last !== undefined && lo <= last[1] + 1) last[1] = Math.max(last[1], hi);
    else out.push([lo, hi]);
  }
  return out;
}

/** Segments back to ranges, joining a run that ends at `RING_MAX` with one that starts at 0. */
function fromSegments(segments: readonly Segment[]): RingRange[] {
  const merged = normalize([...segments]);
  if (merged.length === 0) return [];
  if (merged.length === 1 && merged[0]![0] === 0 && merged[0]![1] === RING_MAX) {
    return [RingRange.full];
  }
  const first = merged[0]!;
  const last = merged[merged.length - 1]!;
  const ranges: RingRange[] = [];
  let inner = merged;
  if (merged.length > 1 && first[0] === 0 && last[1] === RING_MAX) {
    ranges.push(RingRange.of(last[0] - 1, first[1]));
    inner = merged.slice(1, -1);
  }
  for (const [lo, hi] of inner) ranges.push(RingRange.of(lo - 1, hi));
  return ranges;
}

/** Whether any range in `ranges` holds `point`. */
export function rangesContain(ranges: readonly RingRange[], point: number): boolean {
  return ranges.some((r) => r.contains(point));
}

/** Whether any range in `a` shares a point with any range in `b`. */
export function rangesIntersect(a: readonly RingRange[], b: readonly RingRange[]): boolean {
  return a.some((x) => b.some((y) => x.intersects(y)));
}

/** The points in `a` that are in `b`, as ranges. */
export function intersectRanges(a: readonly RingRange[], b: readonly RingRange[]): RingRange[] {
  return fromSegments(
    intersectSegments(a.flatMap(RingRange.segmentsOf), b.flatMap(RingRange.segmentsOf)),
  );
}

/** The points in `a` that are not in `b`, as ranges. */
export function subtractRanges(a: readonly RingRange[], b: readonly RingRange[]): RingRange[] {
  return fromSegments(
    subtractSegments(a.flatMap(RingRange.segmentsOf), b.flatMap(RingRange.segmentsOf)),
  );
}
