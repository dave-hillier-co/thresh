import { grain, reducerState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { ReducerState } from "@tsva/core/reducer-state";
import {
  IAccount,
  initialAccount,
  reduceAccount,
  type AccountEvent,
  type AccountState,
} from "@tsva/example-bank/interfaces";

/**
 * A bank account as a reducer grain (ADR 0006): command handlers validate, then
 * `raise` past-tense events that fold through the pure reducer into immutable
 * state. Snapshot mode persists the folded state via `GrainStorage`; the events
 * are transient. Single-threaded turns mean the fold needs no lock.
 */
@grain()
export class AccountGrain extends Grain implements IAccount {
  @reducerState<AccountState, AccountEvent>("account", {
    initial: initialAccount,
    reduce: reduceAccount,
  })
  private state!: ReducerState<AccountState, AccountEvent>;

  async deposit(cents: number): Promise<number> {
    if (cents <= 0) throw new Error("deposit must be positive");
    this.state.raise({ kind: "deposited", cents });
    await this.state.write();
    return this.state.value.balanceCents;
  }

  async withdraw(cents: number): Promise<number> {
    if (cents <= 0) throw new Error("withdrawal must be positive");
    if (cents > this.state.value.balanceCents) throw new Error("insufficient funds");
    this.state.raise({ kind: "withdrawn", cents });
    await this.state.write();
    return this.state.value.balanceCents;
  }

  async transferTo(other: string, cents: number): Promise<number> {
    // Two steps, not an atomic distributed transaction (a non-goal): withdraw
    // here, then credit the other account through its grain reference.
    const remaining = await this.withdraw(cents);
    await this.getGrain(IAccount, other).deposit(cents);
    return remaining;
  }

  async statement(): Promise<AccountState> {
    return this.state.value; // immutable snapshot
  }
}
