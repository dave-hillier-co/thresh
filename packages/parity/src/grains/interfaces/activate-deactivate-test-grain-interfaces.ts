// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IActivateDeactivateWatcherGrain.cs
// and IActivateDeactivateTestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IActivateDeactivateWatcherGrain extends GrainWithIntegerKey {
  getActivateCalls(): Promise<string[]>;
  getDeactivateCalls(): Promise<string[]>;
  clear(): Promise<void>;
  recordActivateCall(activation: string): Promise<void>;
  recordDeactivateCall(activation: string): Promise<void>;
}

export const IActivateDeactivateWatcherGrain =
  defineGrainInterface<IActivateDeactivateWatcherGrain>(
    "UnitTests.GrainInterfaces.IActivateDeactivateWatcherGrain",
  );

// Note: upstream self-manages activate/deactivate watching via copy-pasted
// grain classes rather than subclassing, since a grain can implement only one
// primary interface; the TS grains below mirror that structure.

export interface ISimpleActivateDeactivateTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
  doDeactivate(): Promise<void>;
}

export const ISimpleActivateDeactivateTestGrain =
  defineGrainInterface<ISimpleActivateDeactivateTestGrain>(
    "UnitTests.GrainInterfaces.ISimpleActivateDeactivateTestGrain",
  );

// Upstream's tail-call variant exercises a .NET IL tail-call code path in
// OnActivateAsync/OnDeactivateAsync; JS has no equivalent distinction, so this
// grain's behaviour mirrors ISimpleActivateDeactivateTestGrain exactly.
export interface ITailCallActivateDeactivateTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
  doDeactivate(): Promise<void>;
}

export const ITailCallActivateDeactivateTestGrain =
  defineGrainInterface<ITailCallActivateDeactivateTestGrain>(
    "UnitTests.GrainInterfaces.ITailCallActivateDeactivateTestGrain",
  );

export interface ILongRunningActivateDeactivateTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
  doDeactivate(): Promise<void>;
}

export const ILongRunningActivateDeactivateTestGrain =
  defineGrainInterface<ILongRunningActivateDeactivateTestGrain>(
    "UnitTests.GrainInterfaces.ILongRunningActivateDeactivateTestGrain",
  );

export interface IBadActivateDeactivateTestGrain extends GrainWithIntegerKey {
  throwSomething(): Promise<void>;
  getKey(): Promise<bigint>;
}

export const IBadActivateDeactivateTestGrain =
  defineGrainInterface<IBadActivateDeactivateTestGrain>(
    "UnitTests.GrainInterfaces.IBadActivateDeactivateTestGrain",
  );

export interface IBadConstructorTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
}

export const IBadConstructorTestGrain = defineGrainInterface<IBadConstructorTestGrain>(
  "UnitTests.GrainInterfaces.IBadConstructorTestGrain",
);

export interface ITaskActionActivateDeactivateTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
  doDeactivate(): Promise<void>;
}

export const ITaskActionActivateDeactivateTestGrain =
  defineGrainInterface<ITaskActionActivateDeactivateTestGrain>(
    "UnitTests.GrainInterfaces.ITaskActionActivateDeactivateTestGrain",
  );

export interface ICreateGrainReferenceTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
  forwardCall(otherGrain: IBadActivateDeactivateTestGrain): Promise<void>;
}

export const ICreateGrainReferenceTestGrain = defineGrainInterface<ICreateGrainReferenceTestGrain>(
  "UnitTests.GrainInterfaces.ICreateGrainReferenceTestGrain",
);

export interface IDeactivatingWhileActivatingTestGrain extends GrainWithIntegerKey {
  doSomething(): Promise<string>;
}

export const IDeactivatingWhileActivatingTestGrain =
  defineGrainInterface<IDeactivatingWhileActivatingTestGrain>(
    "UnitTests.GrainInterfaces.IDeactivatingWhileActivatingTestGrain",
  );
