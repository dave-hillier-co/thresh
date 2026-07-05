// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IGenericInterfaces.cs @ v10.1.0 (MIT).
// Only the non-generic members these grains actually need are ported; every
// generic interface in the upstream file is unrepresentable here
// (GAP-GENERIC-GRAINS — see generic-grain-tests.test.ts).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IGrainWithNoProperties extends GrainWithIntegerKey {
  getAxB(a: number, b: number): Promise<string>;
}

export const IGrainWithNoProperties = defineGrainInterface<IGrainWithNoProperties>(
  "UnitTests.GrainInterfaces.IGrainWithNoProperties",
);

export interface IGrainWithListFields extends GrainWithIntegerKey {
  addItem(item: string): Promise<void>;
  getItems(): Promise<readonly string[]>;
}

export const IGrainWithListFields = defineGrainInterface<IGrainWithListFields>(
  "UnitTests.GrainInterfaces.IGrainWithListFields",
);
