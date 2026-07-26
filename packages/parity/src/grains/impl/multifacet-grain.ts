// Ported from dotnet/orleans test/Grains/TestInternalGrains/MultifacetTestGrain.cs,
// MultifacetFactoryTestGrain.cs @ v10.1.0 (MIT).
// Upstream persists `Reader`/`Writer` grain references in storage-backed state;
// the ported tests only observe state within one activation, so in-memory
// fields suffice (see chained-grain.ts for the same simplification).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IMultifacetFactoryTestGrain,
  IMultifacetReader,
  IMultifacetTestGrain,
  IMultifacetWriter,
} from "@thresh/parity/grains/interfaces/multifacet-grain-interfaces";

export { IMultifacetFactoryTestGrain, IMultifacetReader, IMultifacetTestGrain, IMultifacetWriter };

@grain({ name: "UnitTests.Grains.MultifacetTestGrain" })
export class MultifacetTestGrain extends Grain implements IMultifacetTestGrain {
  private value = 0;

  async setValue(x: number): Promise<void> {
    this.value = x;
  }

  async getValue(): Promise<number> {
    return this.value;
  }
}

@grain({ name: "UnitTests.Grains.MultifacetFactoryTestGrain" })
export class MultifacetFactoryTestGrain extends Grain implements IMultifacetFactoryTestGrain {
  private reader: IMultifacetReader | null = null;
  private writer: IMultifacetWriter | null = null;

  async getReaderOf(grain: IMultifacetTestGrain): Promise<IMultifacetReader> {
    return grain;
  }

  async getReader(): Promise<IMultifacetReader | null> {
    return this.reader;
  }

  async getWriterOf(grain: IMultifacetTestGrain): Promise<IMultifacetWriter> {
    return grain;
  }

  async getWriter(): Promise<IMultifacetWriter | null> {
    return this.writer;
  }

  async setReader(reader: IMultifacetReader): Promise<void> {
    this.reader = reader;
  }

  async setWriter(writer: IMultifacetWriter): Promise<void> {
    this.writer = writer;
  }
}
