// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/FrequencyFilterTests.cs @ v10.1.0 (MIT).
//
// `GetExpectedTopK` drives a Zipf-distributed synthetic workload through the
// repartitioner's frequency-tracking `FrequencyFilter` (a probabilistic
// top-K heavy-hitter sketch) and asserts it recovers the expected top-K keys
// with acceptable error. This framework has no repartitioning subsystem at
// all, so `FrequencyFilter` does not exist (GAP-ACTIVATION-REPARTITIONING,
// see `default-tolerance-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.FrequencyFilterTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.FrequencyFilterTests.GetExpectedTopK",
  );
});
