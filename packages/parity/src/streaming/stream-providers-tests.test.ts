// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StreamProvidersTests.cs @ v10.1.0 (MIT).
//
// `ProvidersTests_ConfigNotLoaded` asserts a specific typed exception
// (`KeyNotFoundException`) when a consumer grain's stream provider lookup
// resolves to a provider whose configuration is missing from the silo's DI
// container (a distinct .NET config-loading failure mode from "no provider
// configured at all"). This framework has no config-loading layer to
// distinguish from a plain missing-provider error — `getStreamProvider`
// throws one generic `Error` either way (`grain-runtime-impl.ts`) — and no
// exception-type hierarchy to assert against, so there is nothing to port
// the specific typed-exception assertion to.
//
// `ServiceId_ProviderRuntime` and `ServiceId_SiloRestart` depend on Orleans'
// `ServiceId` — a stable cluster identifier configured on `TestClusterBuilder`
// and read back via `Client.GetTestHooks(siloHandle).GetServiceId()`, verified
// to survive a full silo restart. This framework has no `ServiceId` concept
// anywhere (no equivalent config option, no test-hooks surface to read it
// back from a running silo).
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const NS = "UnitTests.Streaming.StreamProvidersTests_ProviderConfigNotLoaded";

orleansTest.gap("GAP-STREAM-PROVIDER-CONFIG", `${NS}.ProvidersTests_ConfigNotLoaded`);
orleansTest.gap("GAP-SERVICE-ID", `${NS}.ServiceId_ProviderRuntime`);
orleansTest.gap("GAP-SERVICE-ID", `${NS}.ServiceId_SiloRestart`);

// vitest requires at least one runtime test per file; all upstream Facts are
// gaps above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
