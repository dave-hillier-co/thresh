// Test-scaffolding grain interface for Orleans.Journaling.Tests, standing in for
// upstream's ITestDurableGrain / ITestDurableGrainWithComplexState /
// ITestMultiCollectionGrain / ITestDurableGrainInterface (test/Grains/TestGrains).
// One composite grain exercises a DurableValue, DurableDictionary, DurableList,
// DurableQueue and DurableSet through generic operations, so the journaling
// parity suite can drive every durable structure through a real grain
// activation instead of reaching for the internal state-machine-manager
// package directly (out of bounds for @tsva/parity).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

export interface IDurableCollectionsGrain extends GrainWithStringKey {
  // DurableValue<unknown>
  setValue(v: unknown): Promise<void>;
  getValue(): Promise<unknown>;
  clearValue(): Promise<void>;

  // DurableDictionary<unknown, unknown>
  dictSet(key: unknown, value: unknown): Promise<void>;
  dictGet(key: unknown): Promise<unknown>;
  dictDelete(key: unknown): Promise<boolean>;
  dictHas(key: unknown): Promise<boolean>;
  dictClear(): Promise<void>;
  dictSize(): Promise<number>;
  dictKeys(): Promise<unknown[]>;
  dictValues(): Promise<unknown[]>;
  dictEntries(): Promise<[unknown, unknown][]>;

  // DurableList<unknown>
  listAdd(v: unknown): Promise<void>;
  listSet(i: number, v: unknown): Promise<void>;
  listRemoveAt(i: number): Promise<void>;
  listGet(i: number): Promise<unknown>;
  listClear(): Promise<void>;
  listLength(): Promise<number>;
  listToArray(): Promise<unknown[]>;

  // DurableQueue<unknown>
  queueEnqueue(v: unknown): Promise<void>;
  queueDequeue(): Promise<unknown>;
  queuePeek(): Promise<unknown>;
  queueClear(): Promise<void>;
  queueSize(): Promise<number>;
  queueToArray(): Promise<unknown[]>;

  // DurableSet<unknown>
  setAdd(v: unknown): Promise<boolean>;
  setDelete(v: unknown): Promise<boolean>;
  setClear(): Promise<void>;
  setSize(): Promise<number>;
  setToArray(): Promise<unknown[]>;

  /** A per-activation random tag: proves a new activation ran after a silo restart. */
  activationTag(): Promise<string>;
}

export const IDurableCollectionsGrain = defineGrainInterface<IDurableCollectionsGrain>(
  "Orleans.Journaling.Tests.IDurableCollectionsGrain",
);
