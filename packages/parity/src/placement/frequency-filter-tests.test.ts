// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/FrequencyFilterTests.cs @ v10.1.0 (MIT).
//
// `GetExpectedTopK` drives a Zipf-distributed synthetic workload through the
// repartitioner's frequency-tracking `FrequentItemCollection<TKey, TElement>`
// (a probabilistic top-K heavy-hitter sketch) and asserts it recovers a
// non-empty top-K report — the upstream test itself only asserts the
// formatted report is non-empty (it's a diagnostic/manual-inspection test,
// not a precise-count assertion). Ported onto
// `@tsva/runtime/placement/repartitioning/frequent-item-collection`
// (`FrequentItemCollection`), the same generic sketch that backs
// `FrequentEdgeCounterTests`.
import { FrequentItemCollection } from "@tsva/runtime/placement/repartitioning/frequent-item-collection";
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

class UlongFrequentItemCollection extends FrequentItemCollection<bigint> {
  protected override getKey(element: bigint): bigint {
    return element;
  }
  remove(element: bigint): void {
    this.removeCore(element);
  }
}

/**
 * Rejection-sampling Zipf generator, ported from upstream's
 * `FrequencyFilterTests.ZipfRejectionSampler`
 * (https://jasoncrease.medium.com/rejection-sampling-the-zipf-distribution-6b359792cffa).
 */
class ZipfRejectionSampler {
  private readonly t: number;

  constructor(
    private readonly random: () => number,
    cardinality: number,
    private readonly skew: number,
  ) {
    this.t = (Math.pow(cardinality, 1 - skew) - skew) / (1 - skew);
  }

  sample(): number {
    for (;;) {
      const invB = this.bInvCdf(this.random());
      const sampleX = Math.trunc(invB) + 1;
      const yRand = this.random();
      const ratioTop = Math.pow(sampleX, -this.skew);
      const ratioBottom = sampleX <= 1 ? 1 / this.t : Math.pow(invB, -this.skew) / this.t;
      const rat = ratioTop / (ratioBottom * this.t);

      if (yRand < rat) {
        return sampleX;
      }
    }
  }

  private bInvCdf(p: number): number {
    const pt = p * this.t;
    return pt <= 1 ? pt : Math.pow(pt * (1 - this.skew) + this.skew, 1 / (1 - this.skew));
  }
}

describe("UnitTests.ActivationRepartitioningTests.FrequencyFilterTests", () => {
  orleansTest("UnitTests.ActivationRepartitioningTests.FrequencyFilterTests.GetExpectedTopK", () => {
    const numSamples = 10_000;
    const sink = new UlongFrequentItemCollection(100);
    const distribution = new ZipfRejectionSampler(Math.random, 1000, 0.5);

    for (let i = 0; i < numSamples; i++) {
      const sample = BigInt(distribution.sample());
      sink.add(sample);

      if (i === Math.trunc((4 * numSamples) / 5)) {
        sink.remove(3n);
      }
    }

    const allCounters = sink.elements.slice().sort((left, right) => right.count - left.count);
    const lines = allCounters.map(({ element, count, error }) =>
      error === 0 ? `${element}: ${count}` : `${element}: ${count} ε${error}`,
    );
    const result = lines.join("\n");

    expect(result).not.toBe("");
  });
});
