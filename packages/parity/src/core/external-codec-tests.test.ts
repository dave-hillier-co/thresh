// Ported from dotnet/orleans test/Orleans.Core.Tests/Serialization/ExternalCodecTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Serialization.ExternalCodecTests", () => {
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.ExternalCodecTests.NewtonsoftJsonCodec_ExternalSerializer_Client",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.ExternalCodecTests.NewtonsoftJsonCodec_ExternalSerializer_Silo",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.ExternalCodecTests.NewtonsoftJsonCodec_CanModifySerializerSettings",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.ExternalCodecTests.NewtonsoftJsonCodec_DoesNotSerializeFrameworkTypes",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.ExternalCodecTests.SystemTextJsonCodec_DoesNotSerializeFrameworkTypes",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.ExternalCodecTests.ProtocolBuffersCodec_DoesNotSerializeFrameworkTypes",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
