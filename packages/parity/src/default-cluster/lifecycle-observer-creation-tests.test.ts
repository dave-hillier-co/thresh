// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/LifecycleObserverCreationTests.cs @ v10.1.0 (MIT).
// Upstream validates that a silo lifecycle participant (an
// `ILifecycleParticipant<ISiloLifecycle>` registered in DI) can create a grain
// observer (`CreateObjectReference`) from within a silo-startup hook, during
// the `Active` lifecycle stage. `SiloBuilder.addStartupTask` (Orleans
// `IStartupTask`) is this framework's analogous "run code after the silo
// starts, before it's marked ready" hook; `GrainFactoryAccess` (the object it
// hands the task) now also carries `createObjectReference`/
// `deleteObjectReference`, backed by an embedded `ClientNode` gatewayed
// through the same silo (`packages/hosting/src/silo-builder.ts`), so the
// gap this tag used to track — no way to obtain a client, or otherwise host a
// grain observer, from inside a startup task — is closed for
// `useInProcessTransport`-configured silos (as `TestCluster`'s always are).
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { ISimpleGrain, SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { ISimpleGrainObserver } from "@tsva/parity/grains/interfaces/simple-observerable-grain-interfaces";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("DefaultCluster.Tests.LifecycleObserverCreationTests", () => {
  orleansTest(
    "DefaultCluster.Tests.LifecycleObserverCreationTests.LifecycleParticipant_CanCreateGrainObserver",
    async () => {
      let observerCreated = false;
      let grainCalled = false;

      let cluster: TestCluster | undefined;
      try {
        cluster = await TestCluster.start({
          initialSilos: 1,
          grains: [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }],
          configureSilo: (builder) => {
            builder.addStartupTask(async (grains) => {
              // Creating an object reference from within a startup task —
              // the scenario upstream's fix (running lifecycle events via
              // Task.Run instead of on a SystemTarget) made possible.
              const ref = grains.createObjectReference(ISimpleGrainObserver, {
                stateChanged: async () => undefined,
              });
              expect(ref).toBeDefined();
              observerCreated = true;

              // Also verify that a grain call succeeds from inside the hook.
              const grain = grains.getGrain(ISimpleGrain, 123n);
              await grain.setA(50);
              expect(await grain.getA()).toBe(50);
              grainCalled = true;

              grains.deleteObjectReference(ref);
            });
          },
        });

        expect(observerCreated).toBe(true);
        expect(grainCalled).toBe(true);

        // The cluster is operational after startup too.
        const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
        await grain.setA(100);
        expect(await grain.getA()).toBe(100);
      } finally {
        await cluster?.dispose();
      }
    },
  );
});
