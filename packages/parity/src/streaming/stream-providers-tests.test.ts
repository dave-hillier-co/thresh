// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StreamProvidersTests.cs @ v10.1.0 (MIT).
//
// `ProvidersTests_ConfigNotLoaded` asserts a specific typed exception
// (`KeyNotFoundException`) when a consumer grain's stream provider lookup
// resolves to a provider whose configuration is missing from the silo's DI
// container (a distinct .NET config-loading failure mode from "no provider
// configured at all"). This framework has no config-loading layer to
// distinguish from a plain missing-provider error — `getStreamProvider`
// throws one generic `Error` either way (`grain-runtime-impl.ts`) — and no
// exception-type hierarchy to assert against. Asserting a specific CLR
// exception type is a documented deviation (this port matches Orleans'
// behaviour, not its CLR exception classes), so this is excluded rather than a
// buildable gap.
//
// `ServiceId_ProviderRuntime` and `ServiceId_SiloRestart` live in this file
// upstream but don't actually exercise streaming: they only assert that a
// configured `ServiceId` is readable from a running silo and stable across a
// full silo restart. Ported against `TestCluster`'s `serviceId` option and
// `getServiceId()` accessor, with no stream provider configured.
import { expect } from "vitest";
import { TestCluster } from "@thresh/testing/test-cluster";
import { orleansTest } from "@thresh/testing/orleans-test";

orleansTest.excluded(
  "asserts a specific CLR exception type (KeyNotFoundException) for a missing DI-registered provider config — CLR exception classes are a documented deviation; this port has no config-loading layer distinct from a plain missing-provider error",
  "UnitTests.Streaming.StreamProvidersTests_ProviderConfigNotLoaded.ProvidersTests_ConfigNotLoaded",
);

orleansTest(
  "UnitTests.Streaming.StreamProvidersTests_ProviderConfigNotLoaded.ServiceId_ProviderRuntime",
  async () => {
    const configServiceId = "test-service-id";
    const cluster = await TestCluster.start({ serviceId: configServiceId, initialSilos: 1 });
    try {
      const siloHandle = cluster.silos[0]!;
      const serviceId = cluster.getServiceId(siloHandle);
      expect(serviceId).toBe(configServiceId); // "ServiceId active in silo"
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "UnitTests.Streaming.StreamProvidersTests_ProviderConfigNotLoaded.ServiceId_SiloRestart",
  async () => {
    const configServiceId = "test-service-id";
    const cluster = await TestCluster.start({ serviceId: configServiceId, initialSilos: 1 });
    try {
      expect(cluster.serviceId).toBe(configServiceId); // "ServiceId in test config"

      for (const silo of [...cluster.silos]) {
        await cluster.restartSilo(silo);
      }

      const activeSilos = cluster.silos;
      expect(activeSilos.length).toBeGreaterThan(0);

      for (const siloHandle of activeSilos) {
        const serviceId = cluster.getServiceId(siloHandle);
        expect(serviceId).toBe(configServiceId); // "ServiceId active in silo"
      }
    } finally {
      await cluster.dispose();
    }
  },
);
