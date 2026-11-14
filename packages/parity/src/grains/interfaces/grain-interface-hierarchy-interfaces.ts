// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/GrainInterfaceHierarchyIGrains.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** Upstream `IDoSomething`: not itself a grain interface, mixed into the ones below. */
export interface IDoSomething {
  doIt(): Promise<string>;
  setA(a: number): Promise<void>;
  incrementA(): Promise<void>;
  getA(): Promise<number>;
}

export interface IDoSomethingEmptyGrain extends IDoSomething, GrainKey<bigint> {}

export const IDoSomethingEmptyGrain = defineGrainInterface<IDoSomethingEmptyGrain>(
  "TestGrainInterfaces.IDoSomethingEmptyGrain",
  { key: "integer" },
);

export interface IDoSomethingEmptyWithMoreGrain extends IDoSomethingEmptyGrain {
  doMore(): Promise<string>;
}

export const IDoSomethingEmptyWithMoreGrain = defineGrainInterface<IDoSomethingEmptyWithMoreGrain>(
  "TestGrainInterfaces.IDoSomethingEmptyWithMoreGrain",
  { key: "integer" },
);

// Deliberately empty: mirrors the upstream interface-hierarchy shape exactly.
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface IDoSomethingWithMoreEmptyGrain extends IDoSomethingEmptyWithMoreGrain {}

export const IDoSomethingWithMoreEmptyGrain = defineGrainInterface<IDoSomethingWithMoreEmptyGrain>(
  "TestGrainInterfaces.IDoSomethingWithMoreEmptyGrain",
  { key: "integer" },
);

export interface IDoSomethingWithMoreGrain extends IDoSomething, GrainKey<bigint> {
  doThat(): Promise<string>;
  setB(b: number): Promise<void>;
  incrementB(): Promise<void>;
  getB(): Promise<number>;
}

export const IDoSomethingWithMoreGrain = defineGrainInterface<IDoSomethingWithMoreGrain>(
  "TestGrainInterfaces.IDoSomethingWithMoreGrain",
  { key: "integer" },
);

export interface IDoSomethingCombinedGrain
  extends IDoSomethingWithMoreGrain, IDoSomethingWithMoreEmptyGrain {
  setC(c: number): Promise<void>;
  incrementC(): Promise<void>;
  getC(): Promise<number>;
}

export const IDoSomethingCombinedGrain = defineGrainInterface<IDoSomethingCombinedGrain>(
  "TestGrainInterfaces.IDoSomethingCombinedGrain",
  { key: "integer" },
);
