// Ported from dotnet/orleans test/Orleans.Core.Tests/ProviderErrorMessageTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("NonSilo.Tests.ProviderErrorMessageTests", () => {
  orleansTest.excluded(
    "DI/hosting-builder provider-registration error-message formatting has no equivalent in this framework",
    "NonSilo.Tests.ProviderErrorMessageTests.ClientBuilder_IncludesKnownProvidersInErrorMessage",
  );
  orleansTest.excluded(
    "DI/hosting-builder provider-registration error-message formatting has no equivalent in this framework",
    "NonSilo.Tests.ProviderErrorMessageTests.SiloBuilder_IncludesKnownProvidersInErrorMessage",
  );
  orleansTest.excluded(
    "DI/hosting-builder provider-registration error-message formatting has no equivalent in this framework",
    "NonSilo.Tests.ProviderErrorMessageTests.SiloBuilder_IncludesKnownGrainStorageProvidersInErrorMessage",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
