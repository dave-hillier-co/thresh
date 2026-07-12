// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs
// @ v10.1.0 (MIT). Upstream declares `IActivityGrain`/`ActivityGrain` and
// `IPersistentStateActivityGrain`/`PersistentStateActivityGrain` inline in
// the test file itself (along with `ActivityData`, which this port doesn't
// need — activation tracing is asserted from the harness's captured spans,
// not from data the grain reports back).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IActivityGrain extends GrainWithIntegerKey {
  getActivityId(): Promise<string | undefined>;
}

export const IActivityGrain = defineGrainInterface<IActivityGrain>(
  "UnitTests.GrainInterfaces.IActivityGrain",
);

export interface IPersistentStateActivityGrain extends GrainWithIntegerKey {
  getActivityId(): Promise<string | undefined>;
  getStateValue(): Promise<number>;
}

export const IPersistentStateActivityGrain = defineGrainInterface<IPersistentStateActivityGrain>(
  "UnitTests.GrainInterfaces.IPersistentStateActivityGrain",
);
