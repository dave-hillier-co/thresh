// Ported from dotnet/orleans test/Orleans.Core.Tests/ConfigTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ConfigTests", () => {
  orleansTest.excluded(
    ".NET-specific connection-string redaction / IP-address resolution utilities not present in this framework",
    "UnitTests.ConfigTests.Config_AzureConnectionInfo",
  );
  orleansTest.excluded(
    ".NET-specific connection-string redaction / IP-address resolution utilities not present in this framework",
    "UnitTests.ConfigTests.Config_AdoNetConnectionInfo",
  );
  orleansTest.excluded(
    ".NET-specific connection-string redaction / IP-address resolution utilities not present in this framework",
    "UnitTests.ConfigTests.Config_LocalIPAddressFallback_UsesIPv4Loopback",
  );
  orleansTest.excluded(
    ".NET-specific connection-string redaction / IP-address resolution utilities not present in this framework",
    "UnitTests.ConfigTests.Config_LocalIPAddressFallback_UsesIPv6Loopback",
  );
  orleansTest.excluded(
    ".NET-specific connection-string redaction / IP-address resolution utilities not present in this framework",
    "UnitTests.ConfigTests.Config_LocalIPAddressFallback_ThrowsForExplicitInterface",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
