// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IExceptionGrain.cs @ v10.1.0 (MIT).
// Only the methods exercised by ported ExceptionPropagationTests cases are
// declared; the rest need AggregateException (no JS analogue) or custom
// serializer-codec failure injection (.NET serializer internals) — see that
// test file's EXCLUDED entries.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IExceptionGrain extends GrainWithIntegerKey {
  throwsInvalidOperationException(): Promise<void>;
  throwsNullReferenceException(): Promise<void>;
  grainCallToThrowsInvalidOperationException(otherGrainId: bigint): Promise<void>;
  throwsSynchronousInvalidOperationException(): Promise<void>;
}

export const IExceptionGrain = defineGrainInterface<IExceptionGrain>(
  "UnitTests.GrainInterfaces.IExceptionGrain",
);
