// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IMultifacetWriter.cs,
// IMultifacetReader.cs, test/Grains/TestInternalGrainInterfaces/IMultifacetTestGrain.cs,
// IMultifacetFactoryTestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface IMultifacetWriter extends GrainKey<bigint> {
  setValue(x: number): Promise<void>;
}

export const IMultifacetWriter = defineGrainInterface<IMultifacetWriter>(
  "UnitTests.GrainInterfaces.IMultifacetWriter",
);

export interface IMultifacetReader extends GrainKey<bigint> {
  getValue(): Promise<number>;
}

export const IMultifacetReader = defineGrainInterface<IMultifacetReader>(
  "UnitTests.GrainInterfaces.IMultifacetReader",
);

export interface IMultifacetTestGrain extends IMultifacetReader, IMultifacetWriter {}

export const IMultifacetTestGrain = defineGrainInterface<IMultifacetTestGrain>(
  "UnitTests.GrainInterfaces.IMultifacetTestGrain",
);

export interface IMultifacetFactoryTestGrain extends GrainKey<bigint> {
  getReaderOf(grain: IMultifacetTestGrain): Promise<IMultifacetReader>;
  getReader(): Promise<IMultifacetReader | null>;
  getWriterOf(grain: IMultifacetTestGrain): Promise<IMultifacetWriter>;
  getWriter(): Promise<IMultifacetWriter | null>;
  setReader(reader: IMultifacetReader): Promise<void>;
  setWriter(writer: IMultifacetWriter): Promise<void>;
}

export const IMultifacetFactoryTestGrain = defineGrainInterface<IMultifacetFactoryTestGrain>(
  "UnitTests.GrainInterfaces.IMultifacetFactoryTestGrain",
);
