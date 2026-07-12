// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IEchoTaskGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey, GrainWithIntegerKey } from "@tsva/core/key-kinds";
import type { SiloAddress } from "@tsva/core/silo-address";

export interface IEchoGrain extends GrainWithGuidKey {
  getLastEcho(): Promise<string>;
  echo(data: string): Promise<string>;
  echoError(data: string): Promise<string>;
  echoNullable(value: Date | null): Promise<Date | null>;
}

export const IEchoGrain = defineGrainInterface<IEchoGrain>("UnitTests.GrainInterfaces.IEchoGrain");

export interface IEchoTaskGrain extends GrainWithGuidKey {
  getMyIdAsync(): Promise<number>;
  getLastEchoAsync(): Promise<string>;
  echoAsync(data: string): Promise<string>;
  echoErrorAsync(data: string): Promise<string>;
  pingAsync(): Promise<void>;
  /**
   * Blocks well past its response deadline; the caller should observe
   * `GrainCallTimeoutError` (Orleans `[ResponseTimeout("00:00:05")]`).
   */
  blockingCallTimeoutAsync(delaySeconds: number): Promise<number>;
  /** Pings this activation's hosting silo (Orleans `ISiloControl.Ping` via `GetSiloControlReference`). */
  pingLocalSiloAsync(): Promise<void>;
  /** Pings a specific silo by address. */
  pingRemoteSiloAsync(siloAddress: SiloAddress): Promise<void>;
  /** Pings some other silo in the cluster, discovered via `IManagementGrain.GetHosts`. */
  pingOtherSiloAsync(): Promise<void>;
  /** Pings some other cluster member, discovered via `IManagementGrain.GetHosts`. */
  pingClusterMemberAsync(): Promise<void>;
}

export const IEchoTaskGrain = defineGrainInterface<IEchoTaskGrain>(
  "UnitTests.GrainInterfaces.IEchoTaskGrain",
  { options: { blockingCallTimeoutAsync: { responseTimeout: { seconds: 5 } } } },
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
