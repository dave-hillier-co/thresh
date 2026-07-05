// Ported from dotnet/orleans test/Orleans.Core.Tests/Serialization/SerializationTests.ImmutableCollections.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Serialization.SerializationTestsImmutableCollections", () => {
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_Dictionary",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_Array",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_ArrayDefault",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_HashSet",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_List",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_Queue",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_SortedSet",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsImmutableCollections.SerializationTests_ImmutableCollections_SortedDictionary",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
