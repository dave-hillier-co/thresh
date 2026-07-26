// Ported from dotnet/orleans test/Orleans.Core.Tests/Serialization/OrleansJsonSerializerStreamTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("NonSilo.Tests.Serialization.OrleansJsonSerializerStreamTests", () => {
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "NonSilo.Tests.Serialization.OrleansJsonSerializerStreamTests.StreamRoundTrip_SerializesAndDeserializes",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "NonSilo.Tests.Serialization.OrleansJsonSerializerStreamTests.Deserialize_EmptyStream_ReturnsNull",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "NonSilo.Tests.Serialization.OrleansJsonSerializerStreamTests.StreamRoundTrip_AllowsNull",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "NonSilo.Tests.Serialization.OrleansJsonSerializerStreamTests.Deserialize_InvalidJson_Throws",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
