// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/LifecycleObserverCreationTests.cs @ v10.1.0 (MIT).
// Upstream validates that a silo lifecycle participant (an
// `ILifecycleParticipant<ISiloLifecycle>` registered in DI) can create a grain
// observer (`CreateObjectReference`) during a lifecycle stage. This framework
// has neither grain observers / `CreateObjectReference` (GAP-OBSERVERS) nor a
// silo-lifecycle-participant registration hook to run arbitrary startup code
// against — `SiloBuilder` has no DI container or lifecycle-stage API a test
// could hook into the way `ConfigureServices`/`ILifecycleParticipant` do.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.LifecycleObserverCreationTests", () => {
  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.LifecycleObserverCreationTests.LifecycleParticipant_CanCreateGrainObserver",
  );
});
