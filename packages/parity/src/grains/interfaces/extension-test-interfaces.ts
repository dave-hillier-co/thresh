// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IExtensionTestGrain.cs and
// test/Grains/TestGrainInterfaces/ITestExtension.cs @ v10.1.0 (MIT).
//
// The generic variants (`IGenericExtensionTestGrain<T>`, `IGenericGrainWithNonGenericExtension<T>`,
// `IGenericTestExtension<T>`) are not ported here: they need generic grain support
// (GAP-GENERIC-GRAINS), tracked separately from the non-generic extension mechanism.
//
// The three extension interfaces (`ITestExtension`, `ISimpleExtension`, `IAutoExtension`)
// extend `GrainWithIntegerKey` purely so `castGrainReference` has a concrete key kind to
// carry — an extension interface is never itself the target of `cluster.getGrain`, it is
// only reached by casting a reference to a grain that already has one; the extension rides
// the grain's own id, so the declared key kind here is otherwise unused.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface IExtensionTestGrain extends GrainWithIntegerKey {
  installExtension(name: string): Promise<void>;
}

export const IExtensionTestGrain = defineGrainInterface<IExtensionTestGrain>(
  "UnitTests.GrainInterfaces.IExtensionTestGrain",
);

export type INoOpTestGrain = GrainWithIntegerKey;

export const INoOpTestGrain = defineGrainInterface<INoOpTestGrain>(
  "UnitTests.GrainInterfaces.INoOpTestGrain",
);

export interface ITestExtension extends GrainWithIntegerKey {
  checkExtension_1(): Promise<string>;
  checkExtension_2(): Promise<string>;
}

export const ITestExtension = defineGrainInterface<ITestExtension>(
  "UnitTests.GrainInterfaces.ITestExtension",
  { extension: true },
);

export interface ISimpleExtension extends GrainWithIntegerKey {
  checkExtension_1(): Promise<string>;
}

export const ISimpleExtension = defineGrainInterface<ISimpleExtension>(
  "UnitTests.GrainInterfaces.ISimpleExtension",
  { extension: true },
);

export interface IAutoExtension extends GrainWithIntegerKey {
  checkExtension(): Promise<string>;
}

export const IAutoExtension = defineGrainInterface<IAutoExtension>(
  "UnitTests.GrainInterfaces.IAutoExtension",
  { extension: true },
);
