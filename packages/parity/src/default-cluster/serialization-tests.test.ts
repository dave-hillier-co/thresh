// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/SerializationTests/SerializationTests.cs @ v10.1.0 (MIT).
//
// Every test here calls `HostedCluster.DeepCopy` and
// `HostedCluster.RoundTripSerializationForTesting` directly against the Orleans
// serializer's internal copy/round-trip pipeline (value-type semantics, custom
// activators, `[Serializable]`/`[GenerateSerializer]` codegen) without ever
// going through a grain call. This framework has no such serializer-internals
// API surface to poke at directly — it only serializes at the grain-call
// boundary — so none of it is portable.
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const reason =
  "Calls Orleans' internal DeepCopy/RoundTripSerializationForTesting pipeline directly " +
  "(value-type semantics, custom activators) with no grain call involved; this framework " +
  "has no equivalent serializer-internals API to exercise.";

describe("DefaultCluster.Tests.SerializationTests", () => {
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_LargeTestData",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_ValueType_Phase1",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_ValueType_Phase2",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_DefaultActivatorValueTypeWithRequiredField_Phase1",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_DefaultActivatorValueTypeWithRequiredField_Phase2",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_DefaultActivatorValueTypeWithUseActivator_Phase1",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.SerializationTests.Serialization_DefaultActivatorValueTypeWithUseActivator_Phase2",
  );

  // Every upstream Fact in this file is excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
