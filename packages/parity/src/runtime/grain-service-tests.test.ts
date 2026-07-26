// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainServiceTests/GrainServiceTests.cs @ v10.1.0 (MIT).
//
// "Grain services" are a distinct Orleans concept from grains themselves: a
// single per-silo system service (registered via `ISiloConfigurator` /
// `hostBuilder.AddTestGrainService(...)`) with its own Init/Start lifecycle
// that ordinary grains look up and call into, and which can itself expose
// `IGrainExtension`s (`AddGrainExtension<IEchoExtension, EchoExtension>`).
// GAP-GRAIN-SERVICE is closed by `@thresh/runtime/grain-service` (`GrainService`
// base class + per-silo `GrainServiceRegistry`, reached from a grain via
// `GrainRuntime.getGrainService`) built on the same seams as ordinary grain
// extensions (`@thresh/runtime/grain-management-extension`, the closed
// GAP-GRAIN-EXTENSION) — see that module's class doc for the one deviation
// (no cross-silo ring-routed messaging, so each silo runs its own instance).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  GrainServiceTestGrain,
  IGrainServiceTestGrain,
  TEST_GRAIN_SERVICE_NAME,
  TestGrainService,
} from "@thresh/parity/grains/impl/grain-service-test-grain";

describe("Tester.GrainServiceTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: GrainServiceTestGrain, interfaces: [IGrainServiceTestGrain] }],
      configureSilo: (builder) => {
        builder.addGrainService(TEST_GRAIN_SERVICE_NAME, () => new TestGrainService("abc"));
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("Tester.GrainServiceTests.SimpleInvokeGrainService", async () => {
    const grainRef = cluster.getGrain(IGrainServiceTestGrain, 0n);
    const grainId = await grainRef.getHelloWorldUsingCustomService();
    expect(grainId).toBe("Hello World from Test Grain Service");
    const prop = await grainRef.getServiceConfigProperty();
    expect(prop).toBe("abc");
  });

  orleansTest("Tester.GrainServiceTests.GrainServiceWasStarted", async () => {
    const grainRef = cluster.getGrain(IGrainServiceTestGrain, 0n);
    const prop = await grainRef.callHasStarted();
    expect(prop).toBe(true);
  });

  orleansTest("Tester.GrainServiceTests.GrainServiceWasStartedInBackground", async () => {
    const grainRef = cluster.getGrain(IGrainServiceTestGrain, 0n);
    const prop = await grainRef.callHasStartedInBackground();
    expect(prop).toBe(true);
  });

  orleansTest("Tester.GrainServiceTests.GrainServiceWasInit", async () => {
    const grainRef = cluster.getGrain(IGrainServiceTestGrain, 0n);
    const prop = await grainRef.callHasInit();
    expect(prop).toBe(true);
  });

  orleansTest("Tester.GrainServiceTests.GrainServiceExtensionTest", async () => {
    const grainRef = cluster.getGrain(IGrainServiceTestGrain, 0n);
    const what = await grainRef.echoViaExtension("what");
    expect(what).toBe("what");
  });
});
