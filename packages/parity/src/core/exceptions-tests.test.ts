// Ported from dotnet/orleans test/Orleans.Core.Tests/OrleansRuntime/ExceptionsTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.OrleansRuntime.ExceptionsTests", () => {
  orleansTest.excluded(
    ".NET custom-exception binary-serialization round trip (OrleansException via ISerializable) is a .NET serializer internal with no equivalent in this framework",
    "UnitTests.OrleansRuntime.ExceptionsTests.SerializationTests_Exception_Orleans",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
