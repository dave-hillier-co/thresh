// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/EventSourcing/ICountersGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

/** A grain that maintains a number of counters, indexed by a string key. */
export interface ICountersGrain extends GrainWithIntegerKey {
  /** Updates the counter for the given key by the given amount. */
  add(key: string, amount: number, waitForConfirmation: boolean): Promise<void>;
  /** Resets all counters to zero. */
  reset(waitForConfirmation: boolean): Promise<void>;
  /** Retrieves the tentative value of the counter for the given key. */
  getTentativeCount(key: string): Promise<number>;
  /** Retrieves the tentative value of all counters. */
  getTentativeState(): Promise<Readonly<Record<string, number>>>;
  /** Retrieves the confirmed value of all counters. */
  getConfirmedState(): Promise<Readonly<Record<string, number>>>;
  /** Confirms all previously raised events. */
  confirmAllPreviouslyRaisedEvents(): Promise<void>;
}

export const ICountersGrain = defineGrainInterface<ICountersGrain>(
  "TestGrainInterfaces.ICountersGrain",
);
