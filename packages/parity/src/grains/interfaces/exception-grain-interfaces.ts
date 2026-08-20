// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IExceptionGrain.cs @ v10.1.0 (MIT).
// Only the methods exercised by ported ExceptionPropagationTests cases are
// declared; the rest need AggregateException (no JS analogue) or custom
// serializer-codec failure injection (.NET serializer internals) — see that
// test file's EXCLUDED entries.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IExceptionGrain extends GrainKey<bigint> {
  canceled(): Promise<void>;
  throwsInvalidOperationException(): Promise<void>;
  throwsNullReferenceException(): Promise<void>;
  grainCallToThrowsInvalidOperationException(otherGrainId: bigint): Promise<void>;
  throwsSynchronousInvalidOperationException(): Promise<void>;
}

export const IExceptionGrain = defineGrainInterface<IExceptionGrain>(
  "UnitTests.GrainInterfaces.IExceptionGrain",
);
