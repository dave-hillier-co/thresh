// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IEchoTaskGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey, GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IEchoGrain extends GrainWithGuidKey {
  getLastEcho(): Promise<string>;
  echo(data: string): Promise<string>;
  echoError(data: string): Promise<string>;
  echoNullable(value: Date | null): Promise<Date | null>;
}

export const IEchoGrain = defineGrainInterface<IEchoGrain>("UnitTests.GrainInterfaces.IEchoGrain");

// Upstream also declares BlockingCallTimeout*/PingLocalSilo*/PingRemoteSilo*
// methods; they need per-call response timeouts and silo-control system
// targets (GAP-CANCELLATION / GAP-MGMT-GRAIN) and are omitted until those
// features exist.
export interface IEchoTaskGrain extends GrainWithGuidKey {
  getMyIdAsync(): Promise<number>;
  getLastEchoAsync(): Promise<string>;
  echoAsync(data: string): Promise<string>;
  echoErrorAsync(data: string): Promise<string>;
  pingAsync(): Promise<void>;
}

export const IEchoTaskGrain = defineGrainInterface<IEchoTaskGrain>(
  "UnitTests.GrainInterfaces.IEchoTaskGrain",
);

export interface IBlockingEchoTaskGrain extends GrainWithIntegerKey {
  getMyId(): Promise<number>;
  getLastEcho(): Promise<string>;
  echo(data: string): Promise<string>;
  callMethodTask_Await(data: string): Promise<string>;
  callMethodAV_Await(data: string): Promise<string>;
}

export const IBlockingEchoTaskGrain = defineGrainInterface<IBlockingEchoTaskGrain>(
  "UnitTests.GrainInterfaces.IBlockingEchoTaskGrain",
);

export interface IReentrantBlockingEchoTaskGrain extends GrainWithIntegerKey {
  getMyId(): Promise<number>;
  getLastEcho(): Promise<string>;
  echo(data: string): Promise<string>;
  callMethodTask_Await(data: string): Promise<string>;
  callMethodAV_Await(data: string): Promise<string>;
}

export const IReentrantBlockingEchoTaskGrain =
  defineGrainInterface<IReentrantBlockingEchoTaskGrain>(
    "UnitTests.GrainInterfaces.IReentrantBlockingEchoTaskGrain",
  );
