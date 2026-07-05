// Ported from dotnet/orleans test/Orleans.Core.Tests/Serialization/SerializationTests.DifferentTypes.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Serialization.SerializationTestsDifferentTypes", () => {
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_DateTime",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_DateTimeOffset",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_RecursiveSerialization",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_CultureInfo",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_CultureInfoList",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple2",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple3",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple4",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple5",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple6",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple7",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.SerializationTestsDifferentTypes.SerializationTests_ValueTuple8",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
