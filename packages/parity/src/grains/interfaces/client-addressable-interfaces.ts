// Ported from dotnet/orleans test/Grains/TestInternalGrainInterfaces/IClientAddressableTestClientObject.cs,
// IClientAddressableTestGrain.cs, IClientAddressableTestProducer.cs,
// IClientAddressableTestRendezvousGrain.cs and
// test/Grains/TestGrainInterfaces/IClientAddressableTestConsumer.cs @ v10.1.0 (MIT).
//
// Client-addressable objects are the same underlying mechanism as grain
// observers (see simple-observerable-grain-interfaces.ts), except the
// client-hosted methods here are request/response: the grain awaits a
// return value from the client object rather than firing-and-forgetting a
// notification. None of these methods are one-way.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey, GrainWithStringKey } from "@thresh/core/key-kinds";

/** Client-hosted object a grain calls back into and awaits a result from. */
export interface IClientAddressableTestClientObject extends GrainWithStringKey {
  onHappyPath(message: string): Promise<string>;
  onSadPath(message: string): Promise<void>;
  onSerialStress(n: number): Promise<number>;
  onParallelStress(n: number): Promise<number>;
}

export const IClientAddressableTestClientObject = defineGrainInterface<IClientAddressableTestClientObject>(
  "UnitTests.GrainInterfaces.IClientAddressableTestClientObject",
);

/** Client-hosted producer a grain polls for data. */
export interface IClientAddressableTestProducer extends GrainWithStringKey {
  poll(): Promise<number>;
}

export const IClientAddressableTestProducer = defineGrainInterface<IClientAddressableTestProducer>(
  "UnitTests.GrainInterfaces.IClientAddressableTestProducer",
);

export interface IClientAddressableTestGrain extends GrainWithIntegerKey {
  setTarget(target: IClientAddressableTestClientObject): Promise<void>;
  happyPath(message: string): Promise<string>;
  sadPath(message: string): Promise<void>;
  microSerialStressTest(iterationCount: number): Promise<void>;
  microParallelStressTest(iterationCount: number): Promise<void>;
}

export const IClientAddressableTestGrain = defineGrainInterface<IClientAddressableTestGrain>(
  "UnitTests.GrainInterfaces.IClientAddressableTestGrain",
);

export interface IClientAddressableTestConsumer extends GrainWithIntegerKey {
  pollProducer(): Promise<number>;
  setup(): Promise<void>;
}

export const IClientAddressableTestConsumer = defineGrainInterface<IClientAddressableTestConsumer>(
  "UnitTests.GrainInterfaces.IClientAddressableTestConsumer",
);

export interface IClientAddressableTestRendezvousGrain extends GrainWithIntegerKey {
  getProducer(): Promise<IClientAddressableTestProducer>;
  setProducer(producer: IClientAddressableTestProducer): Promise<void>;
}

export const IClientAddressableTestRendezvousGrain =
  defineGrainInterface<IClientAddressableTestRendezvousGrain>(
    "UnitTests.GrainInterfaces.IClientAddressableTestRendezvousGrain",
  );
