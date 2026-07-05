// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/MemoryStorageProviderTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  IInitialStateGrain,
  InitialStateGrain,
} from "@tsva/parity/grains/impl/initial-state-grain";
import { INullStateGrain, NullStateGrain } from "@tsva/parity/grains/impl/null-state-grain";

describe("DefaultCluster.Tests.StorageTests.MemoryStorageProviderTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: InitialStateGrain, interfaces: [IInitialStateGrain] },
        { ctor: NullStateGrain, interfaces: [INullStateGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.StorageTests.MemoryStorageProviderTests.MemoryStorageProvider_RestoreStateTest",
    async () => {
      const grainWithState = cluster.getGrain(IInitialStateGrain, 0n);
      expect(await grainWithState.getNames()).not.toBeNull();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.StorageTests.MemoryStorageProviderTests.MemoryStorageProvider_NullState",
    async () => {
      const grainWithState = cluster.getGrain(INullStateGrain, 0n);
      expect(await grainWithState.getState()).not.toBeNull();

      await grainWithState.setStateAndDeactivate(null);
      await grainWithState.setStateAndDeactivate({ name: "Thrall" });
    },
  );

  orleansTest(
    "DefaultCluster.Tests.StorageTests.MemoryStorageProviderTests.MemoryStorageProvider_WriteReadStateTest",
    async () => {
      const grainWithState = cluster.getGrain(IInitialStateGrain, 1n);

      let names = await grainWithState.getNames();
      expect(names).not.toBeNull();
      expect(names).toHaveLength(0);

      // first write
      await grainWithState.addName("Bob");
      names = await grainWithState.getNames();
      expect(names).not.toBeNull();
      expect(names).toHaveLength(1);
      expect(names[0]).toBe("Bob");

      // second write
      await grainWithState.addName("Alice");
      names = await grainWithState.getNames();
      expect(names).not.toBeNull();
      expect(names).toHaveLength(2);
      expect(names[0]).toBe("Bob");
      expect(names[1]).toBe("Alice");
    },
  );

  orleansTest.excluded(
    "exercises Orleans' `MemoryStorageGrain` — the memory storage provider is itself " +
      "an addressable system grain (`IMemoryStorageGrain`) reachable via GetGrain; this " +
      "framework's `GrainStorage` is a plain object interface with no grain reference or " +
      "`MemoryStorageEtagMismatchException` type to exercise through one",
    "DefaultCluster.Tests.StorageTests.MemoryStorageProviderTests.MemoryStorageGrainEnforcesEtagsTest",
  );
});
