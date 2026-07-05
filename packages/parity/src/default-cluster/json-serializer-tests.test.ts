// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/SerializationTests/JsonSerializerTests.cs @ v10.1.0 (MIT).
//
// Every test here calls `HostedCluster.RoundTripSystemTextJsonSerialization`
// directly on Orleans' `GrainId` type (and types embedding it in maps/sets),
// exercising System.Text.Json's own converter for that .NET-specific runtime
// type outside of any grain call. This framework's grain identity type has no
// System.Text.Json converter to test (there is no System.Text.Json in a
// TypeScript runtime), and there is no direct serializer-internals entry point
// exposed by the test harness to call in its place.
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const reason =
  "Exercises System.Text.Json's converter for Orleans' .NET GrainId type via " +
  "HostedCluster.RoundTripSystemTextJsonSerialization, called directly outside any grain " +
  "call; no System.Text.Json (or equivalent direct serializer-internals entry point) exists " +
  "in this TypeScript framework.";

describe("DefaultCluster.Tests.JsonSerializerTests", () => {
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.JsonSerializerTests.Serialization_GrainId_RoundTrip",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.JsonSerializerTests.Serialization_WithGrainIdType_RoundTrip",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.JsonSerializerTests.Serialization_WithGrainIdMapType_RoundTrip",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.JsonSerializerTests.Serialization_WithGrainIdConcurrentDictionaryType_RoundTrip",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.JsonSerializerTests.Serialization_WithGrainIdHashSetType_RoundTrip",
  );

  // Every upstream Fact in this file is excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
