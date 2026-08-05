// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IChainedGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** Upstream `ChainGrainHolder`: wraps a grain reference so it round-trips inside a nested object. */
export interface ChainGrainHolder {
  next: IChainedGrain | null;
}

export interface IChainedGrain extends GrainKey<bigint> {
  getId(): Promise<number>;
  getX(): Promise<number>;
  getNext(): Promise<IChainedGrain | null>;
  getCalculatedValue(): Promise<number>;
  setNext(next: IChainedGrain | null): Promise<void>;
  setNextNested(next: ChainGrainHolder): Promise<void>;
  validate(nextIsSet: boolean): Promise<void>;
  passThis(next: IChainedGrain): Promise<void>;
  passNull(next: IChainedGrain): Promise<void>;
  passThisNested(next: ChainGrainHolder): Promise<void>;
  passNullNested(next: ChainGrainHolder): Promise<void>;
}

export const IChainedGrain = defineGrainInterface<IChainedGrain>(
  "UnitTests.GrainInterfaces.IChainedGrain",
);
