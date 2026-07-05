// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/GetGrainInterfaces.cs @ v10.1.0 (MIT).
// Upstream also declares IBase/IDerivedFromBase/IBase1-4 to exercise reflection-based
// ambiguous-implementation resolution (ClassPrefix / FullName GetGrain overloads);
// this framework binds a grain interface to its implementation statically at
// registration time, so that mechanism has no analogue here (see
// grain-factory-tests.test.ts). Only the two key-kind probes are ported.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey, GrainWithStringKey } from "@tsva/core/key-kinds";

export interface IStringGrain extends GrainWithStringKey {
  foo(): Promise<boolean>;
}

export const IStringGrain = defineGrainInterface<IStringGrain>(
  "UnitTests.GrainInterfaces.IStringGrain",
);

export interface IGuidGrain extends GrainWithGuidKey {
  foo(): Promise<boolean>;
}

export const IGuidGrain = defineGrainInterface<IGuidGrain>("UnitTests.GrainInterfaces.IGuidGrain");
