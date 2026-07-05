// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/BlockedBloomFilterTests.cs @ v10.1.0 (MIT).
//
// Two classes of pure data-structure unit tests supporting the activation
// repartitioner: `BlockedBloomFilterTests` exercises the probabilistic
// `BlockedBloomFilter(capacity, errorRate)` (`Add`/`Contains`), and
// `AnchoredGrainsFilterTests` exercises `AnchoredGrainsFilter`'s generational
// rotation (`Add`/`Contains`/`Rotate`/`Reset` across 2- and 3-generation
// configurations). This framework has no repartitioning subsystem at all, so
// neither type exists (GAP-ACTIVATION-REPARTITIONING, see
// `default-tolerance-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.BlockedBloomFilterTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.BlockedBloomFilterTests.AddAndCheck",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.BlockedBloomFilterTests.DoesNotContainSome",
  );
});

describe("UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.AddAndCheck",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_RetainsGrains_AfterFirstCycle",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_DropsGrains_AfterAllCycles_2Gen",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_DropsGrains_AfterAllCycles_3Gen",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.ContinuousActivity_KeepsGrainsAnchored",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Reset_ClearsAllFilters",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.AnchoredGrainsFilterTests.Rotate_CalledManyTimes_DoesNotThrowOutOfBounds",
  );
});
