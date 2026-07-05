// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/HostedClientTests.cs @ v10.1.0 (MIT).
//
// The whole fixture exists to exercise a .NET-specific integration surface:
// `Microsoft.Extensions.Hosting.IHost` + `UseOrleans(...)` wiring a silo and
// an in-process `IClusterClient` into the same DI container/host lifetime
// (`Host.CreateApplicationBuilder().UseOrleans(...)`, `IAsyncLifetime`
// fixture start/stop of the `IHost`). This framework has no hosting-builder
// or DI-container concept to host a silo or client inside — `TestCluster`
// already *is* the in-process analogue every other DefaultCluster suite uses
// directly, so there is nothing hosting-specific left to port once the
// DI/hosting wiring itself is excluded. Individual assertions within these
// tests (grain calls, response timeouts, reference-equality marshaling,
// observers, streams) are otherwise covered by other ported suites.
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.General.HostedClientTests", () => {
  orleansTest.excluded(
    ".NET-specific: exercises Microsoft.Extensions.Hosting IHost/UseOrleans DI wiring of an in-process silo+client, not a grain behavior this framework can host differently",
    "DefaultCluster.Tests.General.HostedClientTests.HostedClient_GrainCallTest",
  );

  orleansTest.excluded(
    ".NET-specific: exercises Microsoft.Extensions.Hosting IHost/UseOrleans DI wiring of an in-process silo+client, not a grain behavior this framework can host differently",
    "DefaultCluster.Tests.General.HostedClientTests.HostedClient_TimeoutTest",
  );

  orleansTest.excluded(
    ".NET-specific: exercises Microsoft.Extensions.Hosting IHost/UseOrleans DI wiring of an in-process silo+client, not a grain behavior this framework can host differently",
    "DefaultCluster.Tests.General.HostedClientTests.HostedClient_ReferenceEquality_GrainCallTest",
  );

  orleansTest.excluded(
    ".NET-specific: exercises Microsoft.Extensions.Hosting IHost/UseOrleans DI wiring of an in-process silo+client, not a grain behavior this framework can host differently",
    "DefaultCluster.Tests.General.HostedClientTests.HostedClient_ObserverTest",
  );

  orleansTest.excluded(
    ".NET-specific: exercises Microsoft.Extensions.Hosting IHost/UseOrleans DI wiring of an in-process silo+client, not a grain behavior this framework can host differently",
    "DefaultCluster.Tests.General.HostedClientTests.HostedClient_StreamTest",
  );

  // Every upstream Fact in this file is excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
