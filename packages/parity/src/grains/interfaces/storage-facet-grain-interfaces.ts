// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StorageFacet/StorageFacetGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IStorageFacetGrain extends GrainKey<bigint> {
  getNames(): Promise<string[]>;
  getExtendedInfo(): Promise<string[]>;
}

export const IStorageFacetGrain = defineGrainInterface<IStorageFacetGrain>(
  "Tester.IStorageFacetGrain",
  { key: "integer" },
);

export type IStorageFactoryGrain = IStorageFacetGrain;
export const IStorageFactoryGrain = defineGrainInterface<IStorageFactoryGrain>(
  "Tester.IStorageFactoryGrain",
  { key: "integer" },
);

export type IStorageDefaultFacetGrain = IStorageFacetGrain;
export const IStorageDefaultFacetGrain = defineGrainInterface<IStorageDefaultFacetGrain>(
  "Tester.IStorageDefaultFacetGrain",
  { key: "integer" },
);

export type IStorageDefaultFactoryGrain = IStorageFacetGrain;
export const IStorageDefaultFactoryGrain = defineGrainInterface<IStorageDefaultFactoryGrain>(
  "Tester.IStorageDefaultFactoryGrain",
  { key: "integer" },
);
