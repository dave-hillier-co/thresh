// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ControllableStreamProviderTests.cs @ v10.1.0 (MIT).
//
// Both tests drive `IManagementGrain.SendControlCommandToProvider<PersistentStreamProvider>`
// — a silo-control system-target call that reaches into a running persistent
// stream provider's adapter/adapter-factory (here, a test-only
// `ControllableTestAdapterFactory`) and asks it to echo a command back,
// across every silo. This framework has no management grain / silo-control
// system-target surface at all (tracked as `GAP-MGMT-GRAIN`), no persistent/
// queue-adapter stream provider to address by name, and no `IControllable`
// adapter to echo a command through.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const NS = "UnitTests.StreamingTests.ControllableStreamProviderTests";

orleansTest.gap("GAP-MGMT-GRAIN", `${NS}.ControllableAdapterEchoTest`);
orleansTest.gap("GAP-MGMT-GRAIN", `${NS}.ControllableAdapterFactoryEchoTest`);

// vitest requires at least one runtime test per file; both upstream Facts are
// gaps above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
