// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/BlockedBloomFilterTests.cs @ v10.1.0 (MIT).
//
// Two classes of pure data-structure unit tests supporting the activation
// repartitioner: `BlockedBloomFilterTests` exercises the probabilistic
// `BlockedBloomFilter(capacity, errorRate)` (`Add`/`Contains`), and
// `AnchoredGrainsFilterTests` exercises `AnchoredGrainsFilter`'s generational
// rotation (`Add`/`Contains`/`Rotate`/`Reset` across 2- and 3-generation
// configurations). Ported onto
// `@tsva/runtime/placement/repartitioning/blocked-bloom-filter`
// (`BlockedBloomFilter`/`AnchoredGrainsFilter`), a faithful port of the
// block/mask structure and generational rotation; see that module's header
// for the one deliberate simplification (a portable 64-bit hash in place of
// .NET's `XxHash3`, which does not change observable add/contains/rotate
// behavior).
import { GrainId } from "@tsva/core/grain-id";
import {
  AnchoredGrainsFilter,
  BlockedBloomFilter,
} from "@tsva/runtime/placement/repartitioning/blocked-bloom-filter";
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const grainId = new GrainId("test", "key");

describe("UnitTests.ActivationRepartitioningTests.BlockedBloomFilterTests", () => {
  orleansTest("UnitTests.ActivationRepartitioningTests.BlockedBloomFilterTests.AddAndCheck", () => {
    const filter = new BlockedBloomFilter(100, 0.01);

    filter.add(grainId);

    expect(filter.contains(grainId)).toBe(true);
  });

  orleansTest("UnitTests.ActivationRepartitioningTests.BlockedBloomFilterTests.DoesNotContainSome", () => {
    const filter = new BlockedBloomFilter(100, 0.01);
    expect(filter.contains(grainId)).toBe(false);
  });
});

describe("UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests", () => {
  const createFilter = (generations = 2) => new AnchoredGrainsFilter(100, 0.01, generations);

  orleansTest("UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.AddAndCheck", () => {
    const filter = createFilter();

    filter.add(grainId);

    expect(filter.contains(grainId)).toBe(true);
  });

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_RetainsGrains_AfterFirstCycle",
    () => {
      const filter = createFilter();

      filter.add(grainId);
      filter.rotate(); // Gen 0 -> Gen 1

      expect(filter.contains(grainId)).toBe(true);
    },
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_DropsGrains_AfterAllCycles_2Gen",
    () => {
      const filter = createFilter(2);
      filter.add(grainId);

      filter.rotate(); // Gen 0 -> Gen 1
      expect(filter.contains(grainId)).toBe(true);

      filter.rotate(); // Gen 1 -> Gone
      expect(filter.contains(grainId)).toBe(false);
    },
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_DropsGrains_AfterAllCycles_3Gen",
    () => {
      const filter = createFilter(3);
      filter.add(grainId);

      filter.rotate(); // Gen 0 -> Gen 1
      expect(filter.contains(grainId)).toBe(true);

      filter.rotate(); // Gen 1 -> Gen 2
      expect(filter.contains(grainId)).toBe(true);

      filter.rotate(); // Gen 2 -> Gone
      expect(filter.contains(grainId)).toBe(false);
    },
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.ContinuousActivity_KeepsGrainsAnchored",
    () => {
      const filter = createFilter(3);

      for (let i = 0; i < 10; i++) {
        filter.add(grainId); // A hot grain
        filter.rotate();
        expect(filter.contains(grainId)).toBe(true);
      }
    },
  );

  orleansTest("UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Reset_ClearsAllFilters", () => {
    const filter = createFilter(3);

    filter.add(grainId);
    filter.reset();

    expect(filter.contains(grainId)).toBe(false); // None of the filters should contain it.
  });

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_CalledManyTimes_DoesNotThrowOutOfBounds",
    () => {
      const filter = createFilter(3);

      expect(() => {
        for (let i = 0; i < 100; i++) {
          filter.rotate();
        }
      }).not.toThrow();
    },
  );
});
