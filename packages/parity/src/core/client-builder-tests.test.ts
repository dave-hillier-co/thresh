// Ported from dotnet/orleans test/Orleans.Core.Tests/ClientBuilderTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.ClientBuilderTests", () => {
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.ClientBuilderTests.ClientBuilder_ClusterOptionsTest",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.ClientBuilderTests.ClientBuilder_NoSpecifiedConfigurationTest",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.ClientBuilderTests.ClientBuilder_ThrowsDuringStartupIfNoGrainInterfacesAdded",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.ClientBuilderTests.ClientBuilder_ServiceProviderTest",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.ClientBuilderTests.ClientBuilderThrowsDuringStartupIfSiloBuildersAdded",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.ClientBuilderTests.ClientBuilderWithHotApplicationBuilderThrowsDuringStartupIfSiloBuildersAdded",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
