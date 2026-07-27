import { stableHash32 } from "@thresh/core/hash";
import type { GrainId } from "@thresh/core/grain-id";

/**
 * A cache-line-blocked bloom filter, faithfully structured after Orleans'
 * `BlockedBloomFilter` (`src/Orleans.Runtime/Placement/Repartitioning/BlockedBloomFilter.cs`):
 * each add/contains touches exactly two adjacent 32-bit "blocks", selected by
 * one hash and populated by two 4-bit masks derived from a second, rotated
 * hash. The polynomial regression that tunes the false-positive rate for a
 * requested `[0.1%, 1%]` range is ported with the same coefficients.
 *
 * Deliberate simplification: Orleans hashes the grain key with
 * `System.IO.Hashing.XxHash3`; this port instead derives a 64-bit hash from
 * two independently-salted `stableHash32` calls. The bit-level hash values
 * therefore differ from .NET's, but the block/mask structure — and thus the
 * filter's behavior and false-positive characteristics — matches upstream.
 */

const BLOCK_SIZE = 32; // higher value yields better speed, but at a high cost of space
const LN2_SQUARED = Math.LN2 ** 2; // 0.4804530139182014246671025263266649717305529515945455
const MIN_FP_RATE = 0.001; // 0.1%
const MAX_FP_RATE = 0.01; // 1%

// Regression coefficients (derived via polynomial regression) to match 'fpRate' as the actual deviates
// significantly with lower and lower 'fpRate'. Ported verbatim from upstream.
const COEFFICIENTS: readonly number[] = [
  4.01022531665245e-3, -1.6272682781603145e1, 2.7169897602930665e4, -2.45276989048125e7,
  1.3273846004698063e10, -4.4943809759769805e12, 9.5588839677303638e14, -1.2081452101930328e17,
  6.8958853188430172e18, 2.6889929911921561e20, -7.1061179529975569e22, 4.4109449793357217e24,
  -9.8041203512310751e25,
];

function regressFpRate(fpRate: number): number {
  let temp = 1;
  let result = 0;
  for (const coefficient of COEFFICIENTS) {
    result += coefficient * temp;
    temp *= fpRate;
  }
  return Math.abs(result);
}

/** Derives an (arbitrary, but stable and well-distributed) 64-bit hash for a `GrainId`. */
function hash64(id: GrainId): bigint {
  const seed = id.getUniformHashCode();
  const key = id.toString();
  const hi = stableHash32(`${seed}:${key}`) >>> 0;
  const lo = stableHash32(`${key}:${seed}`) >>> 0;
  return (BigInt(hi) << 32n) | BigInt(lo);
}

function rotateLeft64(value: bigint, shift: number): bigint {
  const s = BigInt(shift & 63);
  const masked = BigInt.asUintN(64, value);
  return BigInt.asUintN(64, (masked << s) | (masked >> (64n - s)));
}

function getBlockIndex(hash: bigint, buckets: number): number {
  const low32 = hash & 0xffffffffn;
  return Number((low32 * BigInt(buckets)) >> 32n);
}

/**
 * Sets the bits of `hash` corresponding to the lower-order bits, and the bits shifted by 6
 * positions to the right.
 */
function computeMask1(hash: bigint): number {
  const h = Number(BigInt.asIntN(32, hash));
  return (1 << h) | (1 << (h >> 6));
}

/**
 * Sets the bits of `hash`, and the bits shifted by 12 and 18 positions to the right.
 */
function computeMask2(hash: bigint): number {
  const h = Number(BigInt.asIntN(32, hash));
  return (1 << (h >> 12)) | (1 << (h >> 18));
}

export class BlockedBloomFilter {
  private readonly blocks: number;
  private readonly filter: Int32Array;

  /**
   * @param capacity The estimated population size.
   * @param fpRate Bounded within `[MIN_FP_RATE, MAX_FP_RATE]`.
   */
  constructor(capacity: number, fpRate: number) {
    if (fpRate < MIN_FP_RATE || fpRate > MAX_FP_RATE) {
      throw new RangeError(
        `False positive rate '${fpRate}', is outside of the allowed range '${MIN_FP_RATE} - ${MAX_FP_RATE}'`,
      );
    }

    const adjFpRate = regressFpRate(fpRate);
    const bits = Math.trunc((-1 * capacity * Math.log(adjFpRate)) / LN2_SQUARED);

    this.blocks = Math.trunc(bits / BLOCK_SIZE);
    this.filter = new Int32Array(this.blocks + 1);
  }

  add(id: GrainId): void {
    let hash = hash64(id);
    const index = getBlockIndex(hash, this.blocks); // important to get index before rotating the hash

    hash ^= rotateLeft64(hash, 32);

    const mask1 = computeMask1(hash);
    const mask2 = computeMask2(hash);

    this.filter[index] = this.filter[index]! | mask1;
    this.filter[index + 1] = this.filter[index + 1]! | mask2;
  }

  contains(id: GrainId): boolean {
    let hash = hash64(id);
    const index = getBlockIndex(hash, this.blocks); // important to get index before rotating the hash

    hash ^= rotateLeft64(hash, 32);

    const block1 = this.filter[index]!;
    const block2 = this.filter[index + 1]!;

    const mask1 = computeMask1(hash);
    const mask2 = computeMask2(hash);

    return (mask1 & block1) === mask1 && (mask2 & block2) === mask2;
  }

  reset(): void {
    this.filter.fill(0);
  }
}

/**
 * A set of `BlockedBloomFilter`s that rotate anchored grains, ported from Orleans'
 * `AnchoredGrainsFilter`. If a grain is "hot" it will likely survive multiple cycles, otherwise
 * it gets dropped out.
 */
export class AnchoredGrainsFilter {
  // Index 0 is the "active" filter. Indexes 1 through N-1 are the "retiring" filters.
  private filters: BlockedBloomFilter[];

  constructor(capacity: number, fpRate: number, generations: number) {
    if (generations < 1) {
      throw new RangeError("generations: Must have at least 1 generation.");
    }

    this.filters = Array.from(
      { length: generations },
      () => new BlockedBloomFilter(capacity, fpRate),
    );
  }

  add(id: GrainId): void {
    this.filters[0]!.add(id);
  }

  contains(id: GrainId): boolean {
    return this.filters.some((filter) => filter.contains(id));
  }

  /**
   * Rotates the inner filters to implement a sort of sliding window. The previous active
   * (index 0) shifts down, remaining "checkable" for n-1 more cycles, then fades unless
   * re-added. This ensures "cold" grains eventually drop out, while "hot" ones stay anchored.
   */
  rotate(): void {
    const length = this.filters.length;

    // Grab the oldest filter and reset it so it becomes the new active one.
    const nextActive = this.filters[length - 1]!;
    nextActive.reset();

    // Shift all existing generations down by 1, iterating backwards so we don't overwrite
    // elements we haven't shifted yet.
    for (let i = length - 1; i > 0; i--) {
      this.filters[i] = this.filters[i - 1]!;
    }

    this.filters[0] = nextActive;
  }

  reset(): void {
    for (const filter of this.filters) {
      filter.reset();
    }
  }
}
