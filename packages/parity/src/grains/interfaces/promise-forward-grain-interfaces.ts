// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IPromiseForwardGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { ISimpleGrain } from "@tsva/parity/grains/interfaces/simple-grain-interfaces";

export type IPromiseForwardGrain = ISimpleGrain;

export const IPromiseForwardGrain = defineGrainInterface<IPromiseForwardGrain>(
  "UnitTests.GrainInterfaces.IPromiseForwardGrain",
);
