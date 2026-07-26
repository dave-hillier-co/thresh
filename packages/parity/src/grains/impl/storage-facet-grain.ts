// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StorageFacet/StorageFacetGrain.cs @ v10.1.0 (MIT).
//
// Upstream distinguishes two DI wiring styles for the same underlying
// facet-factory infrastructure: `StorageFacetGrain`/`StorageDefaultFacetGrain`
// declare `[ExampleStorage(...)]` CONSTRUCTOR PARAMETERS, resolved
// automatically by an `IAttributeToFactoryMapper`; `StorageFactoryGrain`/
// `StorageDefaultFactoryGrain` instead take the raw
// `INamedExampleStorageFactory`/`IExampleStorageFactory` and call `.Create`
// themselves. This framework has no constructor DI at all — grains are
// `new ctor()` — so the FIELD-decorator facet mechanism (`@exampleStorage`)
// is the only integration surface available, and it is exactly the common
// denominator both upstream styles resolve to (both ultimately call the
// registered named factory). All four grains below therefore bind their
// fields with `@exampleStorage`; the "Factory" grains are kept as distinct
// classes only to preserve the four upstream xunit test identities.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IStorageDefaultFacetGrain,
  IStorageDefaultFactoryGrain,
  IStorageFacetGrain,
  IStorageFactoryGrain,
} from "@thresh/parity/grains/interfaces/storage-facet-grain-interfaces";
import { exampleStorage, type ExampleStorage } from "@thresh/parity/support/example-storage";

export {
  IStorageDefaultFacetGrain,
  IStorageDefaultFactoryGrain,
  IStorageFacetGrain,
  IStorageFactoryGrain,
};

@grain({ name: "Tester.StorageFacetGrain" })
export class StorageFacetGrain extends Grain implements IStorageFacetGrain {
  @exampleStorage("Blob", "FirstState")
  private first!: ExampleStorage<string>;

  @exampleStorage("Table")
  private second!: ExampleStorage<string>;

  async getNames(): Promise<string[]> {
    return [this.first.name, this.second.name];
  }

  async getExtendedInfo(): Promise<string[]> {
    return [this.first.getExtendedInfo(), this.second.getExtendedInfo()];
  }
}

@grain({ name: "Tester.StorageFactoryGrain" })
export class StorageFactoryGrain extends Grain implements IStorageFactoryGrain {
  @exampleStorage("Blob", "FirstState")
  private first!: ExampleStorage<string>;

  @exampleStorage("Table")
  private second!: ExampleStorage<string>;

  async getNames(): Promise<string[]> {
    return [this.first.name, this.second.name];
  }

  async getExtendedInfo(): Promise<string[]> {
    return [this.first.getExtendedInfo(), this.second.getExtendedInfo()];
  }
}

@grain({ name: "Tester.StorageDefaultFacetGrain" })
export class StorageDefaultFacetGrain extends Grain implements IStorageDefaultFacetGrain {
  @exampleStorage(undefined, "FirstState")
  private first!: ExampleStorage<string>;

  @exampleStorage()
  private second!: ExampleStorage<string>;

  async getNames(): Promise<string[]> {
    return [this.first.name, this.second.name];
  }

  async getExtendedInfo(): Promise<string[]> {
    return [this.first.getExtendedInfo(), this.second.getExtendedInfo()];
  }
}

@grain({ name: "Tester.StorageDefaultFactoryGrain" })
export class StorageDefaultFactoryGrain extends Grain implements IStorageDefaultFactoryGrain {
  @exampleStorage(undefined, "FirstState")
  private first!: ExampleStorage<string>;

  @exampleStorage()
  private second!: ExampleStorage<string>;

  async getNames(): Promise<string[]> {
    return [this.first.name, this.second.name];
  }

  async getExtendedInfo(): Promise<string[]> {
    return [this.first.getExtendedInfo(), this.second.getExtendedInfo()];
  }
}
