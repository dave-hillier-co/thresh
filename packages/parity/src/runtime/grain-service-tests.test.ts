// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainServiceTests/GrainServiceTests.cs @ v10.1.0 (MIT).
//
// "Grain services" are a distinct Orleans concept from grains themselves: a
// single per-silo system service (registered via `ISiloConfigurator` /
// `hostBuilder.AddTestGrainService(...)`) with its own Init/Start lifecycle
// that ordinary grains look up and call into, and which can itself expose
// `IGrainExtension`s (`AddGrainExtension<IEchoExtension, EchoExtension>`).
// This framework has no per-silo grain-service registration/lifecycle
// mechanism at all (distinct from, and a precondition for, the
// already-tracked GAP-GRAIN-EXTENSION gap on grain extensions themselves) —
// GAP-GRAIN-SERVICE.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.GrainServiceTests", () => {
  orleansTest.gap("GAP-GRAIN-SERVICE", "Tester.GrainServiceTests.SimpleInvokeGrainService");
  orleansTest.gap("GAP-GRAIN-SERVICE", "Tester.GrainServiceTests.GrainServiceWasStarted");
  orleansTest.gap(
    "GAP-GRAIN-SERVICE",
    "Tester.GrainServiceTests.GrainServiceWasStartedInBackground",
  );
  orleansTest.gap("GAP-GRAIN-SERVICE", "Tester.GrainServiceTests.GrainServiceWasInit");
  orleansTest.gap("GAP-GRAIN-SERVICE", "Tester.GrainServiceTests.GrainServiceExtensionTest");
});
