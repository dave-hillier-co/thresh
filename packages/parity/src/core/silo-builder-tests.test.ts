// Ported from dotnet/orleans test/Orleans.Core.Tests/SiloBuilderTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("NonSilo.Tests.SiloBuilderTests", () => {
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilderTest",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilder_GrainCollectionOptionsForZeroSecondsAgeLimitTest",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilder_ClusterMembershipOptionsValidators",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilder_LoadSheddingValidatorAbove100ShouldFail",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilderThrowsDuringStartupIfNoGrainsAdded",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilderThrowsDuringStartupIfClientBuildersAdded",
  );
  orleansTest.excluded(
    "DI/hosting-builder mechanism (HostBuilder/IServiceCollection) has no equivalent in this framework",
    "NonSilo.Tests.SiloBuilderTests.SiloBuilderWithHotApplicationBuilderThrowsDuringStartupIfClientBuildersAdded",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
