// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ProviderTests.cs @ v10.1.0 (MIT).
//
// Every case here turns on `IGrainExtension` / `AsReference<TExtension>()` /
// `AddGrainExtension` — dynamically installing an additional interface onto an
// already-registered grain instance and casting a reference to it. This
// framework has no grain-extension mechanism at all (already tracked as
// GAP-GRAIN-EXTENSION), so all four are gaps.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.ProviderTests", () => {
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "DefaultCluster.Tests.ProviderTests.Providers_TestExtensions",
  );

  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "DefaultCluster.Tests.ProviderTests.Providers_ActivateNonGenericExtensionOfGenericInterface",
  );

  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "DefaultCluster.Tests.ProviderTests.Providers_ReferenceNonGenericExtensionOfGenericInterface",
  );

  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "DefaultCluster.Tests.ProviderTests.Providers_AutoInstallExtensionTest",
  );
});
