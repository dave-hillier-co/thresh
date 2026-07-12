// Ported from dotnet/orleans test/Grains/TestGrains/EventSourcing/{CountersGrain,CountersGrainVariations}.cs @ v10.1.0 (MIT).
// A real `JournaledGrain`: `Add`/`Reset` raise events applied to the tentative
// state immediately, and only reach the confirmed state (and log) once
// confirmed. The four `*_StateStore_*`/`*_LogStore_*` variants exist only to
// pick different grain-type names for the perf-comparison tests; this
// framework has one journal-storage substrate (no separate "persist latest
// state only" vs. "persist the full log" consistency providers), so all four
// share the same `CountersGrain` behaviour and differ only in reentrancy.
import { grain, reentrant } from "@tsva/core/decorators";
import { JournaledGrain } from "@tsva/core/journaled-grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import { ICountersGrain } from "@tsva/parity/grains/interfaces/counters-grain-interfaces";

export { ICountersGrain };

// The interface->grain-type mapping is one-to-one, so the four perf-test
// variants (which need to be independently addressable grain types) each get
// their own `GrainInterface`, identical in shape to `ICountersGrain`.
export const ICountersGrainStateStoreNonReentrant = defineGrainInterface<ICountersGrain>(
  "TestGrainInterfaces.ICountersGrain.StateStoreNonReentrant",
);
export const ICountersGrainStateStoreReentrant = defineGrainInterface<ICountersGrain>(
  "TestGrainInterfaces.ICountersGrain.StateStoreReentrant",
);
export const ICountersGrainLogStoreNonReentrant = defineGrainInterface<ICountersGrain>(
  "TestGrainInterfaces.ICountersGrain.LogStoreNonReentrant",
);
export const ICountersGrainLogStoreReentrant = defineGrainInterface<ICountersGrain>(
  "TestGrainInterfaces.ICountersGrain.LogStoreReentrant",
);

interface CountersState {
  readonly counts: Readonly<Record<string, number>>;
}

type CountersEvent =
  | { readonly kind: "updated"; readonly key: string; readonly amount: number }
  | { readonly kind: "resetAll" };

@grain({ name: "simple-counters-grain" })
export class CountersGrain
  extends JournaledGrain<CountersState, CountersEvent>
  implements ICountersGrain
{
  initialState(): CountersState {
    return { counts: {} };
  }

  transitionState(state: CountersState, event: CountersEvent): CountersState {
    if (event.kind === "resetAll") return { counts: {} };
    const current = state.counts[event.key] ?? 0;
    return { counts: { ...state.counts, [event.key]: current + event.amount } };
  }

  async add(key: string, amount: number, waitForConfirmation: boolean): Promise<void> {
    this.raiseEvent({ kind: "updated", key, amount });
    if (waitForConfirmation) await this.confirmEvents();
  }

  async reset(waitForConfirmation: boolean): Promise<void> {
    this.raiseEvent({ kind: "resetAll" });
    if (waitForConfirmation) await this.confirmEvents();
  }

  async confirmAllPreviouslyRaisedEvents(): Promise<void> {
    await this.confirmEvents();
  }

  async getTentativeCount(key: string): Promise<number> {
    return this.tentativeState.counts[key] ?? 0;
  }

  async getTentativeState(): Promise<Readonly<Record<string, number>>> {
    return this.tentativeState.counts;
  }

  async getConfirmedState(): Promise<Readonly<Record<string, number>>> {
    return this.state.counts;
  }
}

@grain({ name: "TestGrains.CountersGrain_StateStore_NonReentrant" })
export class CountersGrainStateStoreNonReentrant extends CountersGrain {}

@grain({ name: "TestGrains.CountersGrain_StateStore_Reentrant" })
@reentrant()
export class CountersGrainStateStoreReentrant extends CountersGrain {}

@grain({ name: "TestGrains.CountersGrain_LogStore_NonReentrant" })
export class CountersGrainLogStoreNonReentrant extends CountersGrain {}

@grain({ name: "TestGrains.CountersGrain_LogStore_Reentrant" })
@reentrant()
export class CountersGrainLogStoreReentrant extends CountersGrain {}
