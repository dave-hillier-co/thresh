// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StorageFacet/StorageFacetTests.cs @ v10.1.0 (MIT).
//
// Orleans "grain facets" let a grain constructor declare
// `[ExampleStorage("Blob")] IExampleStorage<T> state` parameters that DI
// resolves per-property to independently-configured storage implementations
// (Blob vs Table factories registered via `hostBuilder.UseBlobExampleStorage`
// / `UseTableExampleStorage`, with one nominated the process-wide default via
// `UseAsDefaultExampleStorage<T>`). This framework has no constructor DI, so
// facets bind onto decorated fields instead (see the general
// `@grainFacet`/`addGrainFacetFactory` mechanism in
// `packages/core/src/decorators.ts` / `packages/hosting/src/silo-builder.ts`,
// and the `ExampleStorage` plugin built on it in
// `packages/parity/src/support/example-storage.ts`).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  IStorageDefaultFacetGrain,
  IStorageDefaultFactoryGrain,
  IStorageFacetGrain,
  IStorageFactoryGrain,
  StorageDefaultFacetGrain,
  StorageDefaultFactoryGrain,
  StorageFacetGrain,
  StorageFactoryGrain,
} from "@thresh/parity/grains/impl/storage-facet-grain";
import {
  tableExampleStorageFactory,
  useAsDefaultExampleStorage,
  useBlobExampleStorage,
  useTableExampleStorage,
} from "@thresh/parity/support/example-storage";

describe("Tester.StorageFacetTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: StorageFacetGrain, interfaces: [IStorageFacetGrain] },
        { ctor: StorageFactoryGrain, interfaces: [IStorageFactoryGrain] },
        { ctor: StorageDefaultFacetGrain, interfaces: [IStorageDefaultFacetGrain] },
        { ctor: StorageDefaultFactoryGrain, interfaces: [IStorageDefaultFactoryGrain] },
      ],
      configureSilo: (builder) => {
        // Default storage feature factory - optional.
        useAsDefaultExampleStorage(builder, tableExampleStorageFactory);
        // Named providers.
        useBlobExampleStorage(builder, "Blob");
        useTableExampleStorage(builder, "Table");
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("Tester.StorageFacetTests.ExampleStorageFacetHappyPath", async () => {
    const grain = cluster.getGrain(IStorageFacetGrain, 0n);
    const names = await grain.getNames();
    const info = await grain.getExtendedInfo();
    expect(names).toEqual(["FirstState", "second"]);
    expect(info).toEqual([
      "Blob:FirstState, StateType:String",
      "Table:second-ActivateCalled:true, StateType:String",
    ]);
  });

  orleansTest("Tester.StorageFacetTests.ExampleStorageFactoryHappyPath", async () => {
    const grain = cluster.getGrain(IStorageFactoryGrain, 0n);
    const names = await grain.getNames();
    const info = await grain.getExtendedInfo();
    expect(names).toEqual(["FirstState", "second"]);
    expect(info).toEqual([
      "Blob:FirstState, StateType:String",
      "Table:second-ActivateCalled:true, StateType:String",
    ]);
  });

  orleansTest("Tester.StorageFacetTests.ExampleStorageFacetDefaultPath", async () => {
    const grain = cluster.getGrain(IStorageDefaultFacetGrain, 0n);
    const names = await grain.getNames();
    const info = await grain.getExtendedInfo();
    expect(names).toEqual(["FirstState", "second"]);
    expect(info).toEqual([
      "Table:FirstState-ActivateCalled:true, StateType:String",
      "Table:second-ActivateCalled:true, StateType:String",
    ]);
  });

  orleansTest("Tester.StorageFacetTests.ExampleStorageFactoryDefaultPath", async () => {
    const grain = cluster.getGrain(IStorageDefaultFactoryGrain, 0n);
    const names = await grain.getNames();
    const info = await grain.getExtendedInfo();
    expect(names).toEqual(["FirstState", "second"]);
    expect(info).toEqual([
      "Table:FirstState-ActivateCalled:true, StateType:String",
      "Table:second-ActivateCalled:true, StateType:String",
    ]);
  });
});
