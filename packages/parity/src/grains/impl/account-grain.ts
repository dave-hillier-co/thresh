// Ported from dotnet/orleans test/Grains/TestGrains/EventSourcing/AccountGrain.cs @ v10.1.0 (MIT).
// Upstream `AccountGrain` is a `JournaledGrain<GrainState, Transaction>` with a
// `LogStorage` consistency provider, so `GetTransactionLog` can replay the full
// event history. This framework now has `JournaledGrain` (see
// `@thresh/core/journaled-grain`), but the only ported test exercising a real
// event log (`AccountGrainTests.AccountWithLog`) is skipped upstream too
// (dotnet/orleans#5605), so only the `AccountGrain_PersistStateOnly` variant —
// which keeps just the latest balance and rejects `GetTransactionLog` with
// `NotSupportedException` — is ported here; it needs no event log.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { Guid } from "@thresh/core/guid";
import {
  IAccountGrain,
  type Transaction,
} from "@thresh/parity/grains/interfaces/account-grain-interfaces";

export { IAccountGrain };

@grain({ name: "TestGrains.AccountGrain_PersistStateOnly" })
export class AccountGrainPersistStateOnly extends Grain implements IAccountGrain {
  private currentBalance = 0;

  async balance(): Promise<number> {
    return this.currentBalance;
  }

  async deposit(amount: number, guid: Guid, description: string): Promise<void> {
    this.currentBalance += amount;
    void guid;
    void description;
  }

  async withdraw(amount: number, guid: Guid, description: string): Promise<boolean> {
    if (this.currentBalance < amount) return false;
    this.currentBalance -= amount;
    void guid;
    void description;
    return true;
  }

  async getTransactionLog(): Promise<readonly Transaction[]> {
    throw new Error("NotSupportedException: this grain does not persist a transaction log");
  }
}
