// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/EventSourcing/IAccountGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { Guid } from "@thresh/core/guid";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";

/** A single deposit or withdrawal against the account. */
export interface Transaction {
  guid: Guid;
  description: string;
  issueTime: Date;
  kind: "deposit" | "withdrawal";
  amount: number;
}

export interface IAccountGrain extends GrainWithStringKey {
  balance(): Promise<number>;
  deposit(amount: number, guid: Guid, description: string): Promise<void>;
  withdraw(amount: number, guid: Guid, description: string): Promise<boolean>;
  getTransactionLog(): Promise<readonly Transaction[]>;
}

export const IAccountGrain = defineGrainInterface<IAccountGrain>(
  "UnitTests.GrainInterfaces.IAccountGrain",
);
