// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/LifecycleObserverCreationTests.cs @ v10.1.0 (MIT).
// Upstream validates that a silo lifecycle participant (an
// `ILifecycleParticipant<ISiloLifecycle>` registered in DI) can create a grain
// observer (`CreateObjectReference`) from within a silo-startup hook, during
// the `Active` lifecycle stage. `SiloBuilder.addStartupTask` (Orleans
// `IStartupTask`) is this framework's analogous "run code after the silo
// starts, before it's marked ready" hook, but it is deliberately typed to hand
// the task only `GrainFactoryAccess` — `getGrain`, nothing else (see
// `packages/hosting/src/silo-builder.ts`). `createObjectReference` exists
// solely on the client (`ClientNode`, `packages/client/src/client-node.ts`);
// there is no way to obtain a client — or otherwise host a grain observer —
// from inside a startup task or any other silo-side hook. So this scenario
// stays GAP-OBSERVERS even though client-side observer creation itself is
// fully supported (see `default-cluster/observer.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.LifecycleObserverCreationTests", () => {
  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.LifecycleObserverCreationTests.LifecycleParticipant_CanCreateGrainObserver",
  );
});
