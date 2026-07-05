// Ported from dotnet/orleans test/Orleans.Core.Tests/Serialization/MessageSerializerTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Serialization.MessageSerializerTests", () => {
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.MessageTest_TtlUpdatedOnAccess",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.MessageTest_TtlUpdatedOnSerialization",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.Message_SerializeHeaderTooBig",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.Message_SerializeBodyTooBig",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.Message_DeserializeHeaderTooBig",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.Message_DeserializeBodyTooBig",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.MessageSerializerTests.MessageTest_CacheInvalidationHeader_RoundTripCompatibility",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
