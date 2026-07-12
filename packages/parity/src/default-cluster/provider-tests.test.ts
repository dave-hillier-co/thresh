// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ProviderTests.cs @ v10.1.0 (MIT).
//
// Grain extensions (Orleans `IGrainExtension` / `AsReference<TExtension>()` /
// `AddGrainExtension`) let a grain expose an additional, dynamically-installed
// interface, dispatched independently of the grain's own methods. Two of the
// four upstream cases are ported here; the other two need generic-grain
// support (GAP-GENERIC-GRAINS) — the extension mechanism itself is no longer
// the blocker.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { castGrainReference } from "@tsva/core/grain-reference";
import { GrainExtensionNotInstalledException } from "@tsva/core/errors";
import {
  ExtensionTestGrain,
  NoOpTestGrain,
  AutoExtension,
} from "@tsva/parity/grains/impl/extension-test-grain";
import {
  IExtensionTestGrain,
  INoOpTestGrain,
  ITestExtension,
  ISimpleExtension,
  IAutoExtension,
} from "@tsva/parity/grains/interfaces/extension-test-interfaces";
import { TestGrain, ITestGrain } from "@tsva/parity/grains/impl/test-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("DefaultCluster.Tests.ProviderTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: ExtensionTestGrain, interfaces: [IExtensionTestGrain] },
        { ctor: NoOpTestGrain, interfaces: [INoOpTestGrain] },
        { ctor: TestGrain, interfaces: [ITestGrain] },
      ],
      configureSilo: (builder) => {
        builder.addGrainExtension(IAutoExtension, () => new AutoExtension());
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.ProviderTests.Providers_TestExtensions", async () => {
    const grain = cluster.getGrain(IExtensionTestGrain, randomIntegerKey());
    const extension = castGrainReference(grain, ITestExtension);

    await expect(extension.checkExtension_1()).rejects.toBeInstanceOf(
      GrainExtensionNotInstalledException,
    );

    await grain.installExtension("test");

    expect(await extension.checkExtension_1()).toBe("test");
    expect(await extension.checkExtension_2()).toBe("23");
  });

  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
    "DefaultCluster.Tests.ProviderTests.Providers_ActivateNonGenericExtensionOfGenericInterface",
  );

  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
    "DefaultCluster.Tests.ProviderTests.Providers_ReferenceNonGenericExtensionOfGenericInterface",
  );

  orleansTest("DefaultCluster.Tests.ProviderTests.Providers_AutoInstallExtensionTest", async () => {
    const grain = cluster.getGrain(INoOpTestGrain, randomIntegerKey());
    const uninstalled = castGrainReference(grain, ISimpleExtension);
    const autoInstalled = castGrainReference(grain, IAutoExtension);

    await expect(uninstalled.checkExtension_1()).rejects.toBeInstanceOf(
      GrainExtensionNotInstalledException,
    );
    await expect(autoInstalled.checkExtension()).resolves.toBe("whoot!");
  });
});
