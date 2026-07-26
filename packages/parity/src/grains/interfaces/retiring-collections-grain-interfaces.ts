// Test-scaffolding grain interfaces for GAP-STATE-MACHINE-RETIREMENT
// (Orleans.Journaling.Tests.StateMachineManagerTests.StateMachineManager_AutoRetiringStateMachines).
//
// Upstream constructs several `IStateMachineManager` instances directly, over
// the same storage, registering a different set of `IDurableStateMachine`s
// each time (some tests register a "dictToKeep" and a "dictToRetire", others
// only "dictToKeep") to simulate a durable structure being removed from a
// grain's dependencies across a deploy. Reaching the manager surface directly
// is out of bounds for @thresh/parity (see durable-collections-grain-interfaces.ts),
// so instead this uses two grain *classes* sharing one grain type name
// ("Orleans.Journaling.Tests.RetiringCollectionsGrain"): one with both durable
// dictionaries ("full", `IRetiringCollectionsGrain`), one with only the
// "keep" dictionary ("partial", `IRetiringCollectionsGrainPartial`). Because
// the grain type name is what a `GrainId` is keyed on (not the JS class),
// resolving the same key through the "partial" grain after the "full" one
// (with a shared journal-storage instance, see the test) reproduces the same
// "structure disappeared from the grain's registered set" scenario upstream
// exercises by constructing a manager with fewer machines.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";

export interface IRetiringCollectionsGrainPartial extends GrainWithStringKey {
  keepSet(key: unknown, value: unknown): Promise<void>;
  keepGet(key: unknown): Promise<unknown>;
}

export const IRetiringCollectionsGrainPartial =
  defineGrainInterface<IRetiringCollectionsGrainPartial>(
    "Orleans.Journaling.Tests.IRetiringCollectionsGrainPartial",
  );

export interface IRetiringCollectionsGrain extends IRetiringCollectionsGrainPartial {
  retireSet(key: unknown, value: unknown): Promise<void>;
  retireGet(key: unknown): Promise<unknown>;
  retireHas(key: unknown): Promise<boolean>;
}

export const IRetiringCollectionsGrain = defineGrainInterface<IRetiringCollectionsGrain>(
  "Orleans.Journaling.Tests.IRetiringCollectionsGrain",
);
