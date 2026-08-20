// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/GrainFaultTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// GrainFaultTransactionMemoryTests inherits every [Theory] from
// GrainFaultTransactionTestRunner
// (src/Orleans.Transactions.TestKit.Base/TestRunners/GrainFaultTransactionTestRunner.cs),
// each with 3 InlineData cases over TransactionTestConstants'
// Single/Double/MaxStateTransactionalGrain grain-state shapes. This port
// collapses those 3 state-shape cases to a single Single-state-equivalent
// counter per method (one `orleansTest` id per method, not x3) — the shapes
// only vary how many `[TransactionalState]` fields a grain carries, not the
// exception behaviour under test.
//
// `ITransactionTestGrain` (ITransactionTestGrain.cs) here is a transactional
// counter grain: `set`/`add`/`get`/`addAndThrow`, all `[Transaction(CreateOrJoin)]`.
// `add`/`get` return `int[]` upstream (one entry per `[TransactionalState]`
// field); this single-state port always returns a one-element array.
// `addAndThrow` mirrors `MultiStateTransactionalGrainBaseClass.AddAndThrow`:
// adds, then throws `AddAndThrowException` (`AddAndThrowError` here).
//
// `ITransactionCoordinatorGrain` (ITransactionCoordinatorGrain.cs /
// TransactionCoordinatorGrain.cs) fans out to test grains, all
// `[Transaction(Create)]`: `multiGrainSet` (`Task.WhenAll(grains.Set)`);
// `addAndThrow` (await `grain.Add`, then throw a plain `Exception` — a
// DIFFERENT exception from the grain-level `AddAndThrow` above);
// `multiGrainAddAndThrow` (await `Task.WhenAll(grains.Add)`, then await
// `Task.WhenAll(throwGrains.AddAndThrow)`); `updateViolated`
// (`[Transaction(Create)]` + `[ReadOnly]`, calls `grain.Add` — a write in a
// read-only transaction); `orphanCallTransaction`
// (`TransactionContext.GetRequiredTransactionInfo().Fork()`, never awaited).
//
// Exception mapping (`src/Orleans.Transactions/OrleansTransactionException.cs`
// -> `@thresh/core/errors`): `OrleansTransactionAbortedException` ->
// `TransactionAbortedError`; `OrleansReadOnlyViolatedException` ->
// `TransactionReadOnlyViolatedError`; `OrleansOrphanCallException` ->
// `TransactionOrphanCallError`; `OrleansCascadingAbortException` ->
// `TransactionCascadingAbortError` (swallowed by `testAfterDustSettles`, this
// port's `TestAfterDustSettles`).
import { expect } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import {
  TransactionAbortedError,
  TransactionCascadingAbortError,
  TransactionOrphanCallError,
  TransactionReadOnlyViolatedError,
} from "@thresh/core/errors";
import { TestCluster } from "@thresh/testing/test-cluster";
import { orleansTest } from "@thresh/testing/orleans-test";

const EXPECTED = 5;
/** TransactionTestConstants.MaxCoordinatedTransactions (Orleans @ v10.1.0). */
const MAX_COORDINATED_TRANSACTIONS = 8;

/** Orleans `Orleans.Transactions.TestKit.AddAndThrowException`. */
class AddAndThrowError extends Error {
  constructor(message = "test exception") {
    super(message);
    this.name = "AddAndThrowError";
  }
}

interface Counter {
  value: number;
}

interface FaultTestGrain extends GrainKey<string> {
  set(newValue: number): Promise<void>;
  add(delta: number): Promise<number[]>;
  get(): Promise<number[]>;
  addAndThrow(delta: number): Promise<void>;
}

interface FaultCoordinatorGrain extends GrainKey<string> {
  multiGrainSet(keys: string[], newValue: number): Promise<void>;
  addAndThrow(key: string, delta: number): Promise<void>;
  multiGrainAddAndThrow(throwKeys: string[], keys: string[], delta: number): Promise<void>;
  updateViolated(key: string, delta: number): Promise<void>;
  orphanCallTransaction(): Promise<void>;
}

const FaultTestGrainInterface = defineGrainInterface<FaultTestGrain>("FaultTestGrain", {
  options: {
    set: { transaction: "createOrJoin" },
    add: { transaction: "createOrJoin" },
    get: { transaction: "createOrJoin" },
    addAndThrow: { transaction: "createOrJoin" },
  },
});

const FaultCoordinatorGrainInterface = defineGrainInterface<FaultCoordinatorGrain>(
  "FaultCoordinatorGrain",
  {
    options: {
      multiGrainSet: { transaction: "create" },
      addAndThrow: { transaction: "create" },
      multiGrainAddAndThrow: { transaction: "create" },
      updateViolated: { transaction: "create", readOnly: true },
      orphanCallTransaction: { transaction: "create" },
    },
  },
);

const FaultTestGrainImpl = defineGrain<FaultTestGrain>("FaultTestGrain", () => {
  const data = useTransactionalState<Counter>("data", { initial: () => ({ value: 0 }) });
  return {
    set: (newValue) =>
      data.performUpdate((s) => {
        s.value = newValue;
      }),
    add: (delta) =>
      data.performUpdate((s) => {
        s.value += delta;
        return [s.value];
      }),
    get: () => data.performRead((s) => [s.value]),
    addAndThrow: async (delta) => {
      await data.performUpdate((s) => {
        s.value += delta;
      });
      throw new AddAndThrowError();
    },
  };
});

const FaultCoordinatorGrainImpl = defineGrain<FaultCoordinatorGrain>(
  "FaultCoordinatorGrain",
  (ctx) => {
    const grain = (key: string): FaultTestGrain => ctx.getGrain(FaultTestGrainInterface, key);
    return {
      multiGrainSet: async (keys, newValue) => {
        await Promise.all(keys.map((k) => grain(k).set(newValue)));
      },
      addAndThrow: async (key, delta) => {
        await grain(key).add(delta);
        throw new Error("This should abort the transaction");
      },
      multiGrainAddAndThrow: async (throwKeys, keys, delta) => {
        await Promise.all(keys.map((k) => grain(k).add(delta)));
        await Promise.all(throwKeys.map((k) => grain(k).addAndThrow(delta)));
      },
      updateViolated: async (key, delta) => {
        await grain(key).add(delta);
      },
      orphanCallTransaction: () => {
        ctx.runtime.forkTransaction();
        return Promise.resolve();
      },
    };
  },
);

async function buildCluster(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    grains: [
      { ctor: FaultTestGrainImpl.grain, interfaces: [FaultTestGrainInterface] },
      { ctor: FaultCoordinatorGrainImpl.grain, interfaces: [FaultCoordinatorGrainInterface] },
    ],
  });
}

let nextKey = 0;
const newKey = (prefix: string): string => `${prefix}-${(nextKey += 1)}`;

/**
 * Orleans `GrainFaultTransactionTestRunner.TestAfterDustSettles`: retries
 * `what` up to twice, swallowing `OrleansCascadingAbortException` — an
 * optimistic read can observe a since-aborted transaction's tentative write,
 * which cascades its own abort.
 */
async function testAfterDustSettles(what: () => Promise<void>): Promise<void> {
  let tries = 2;
  while (tries-- > 0) {
    try {
      await what();
      return;
    } catch (error) {
      if (!(error instanceof TransactionCascadingAbortError)) throw error;
    }
  }
}

orleansTest(
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionOnExceptions",
  async () => {
    const cluster = await buildCluster();
    try {
      const key = newKey("grain");
      const grain = cluster.getGrain(FaultTestGrainInterface, key);
      const coordinator = cluster.getGrain(FaultCoordinatorGrainInterface, newKey("coord"));

      await coordinator.multiGrainSet([key], EXPECTED);
      await expect(coordinator.addAndThrow(key, EXPECTED)).rejects.toThrow(TransactionAbortedError);

      await testAfterDustSettles(async () => {
        const actual = await grain.get();
        for (const value of actual) expect(value).toBe(EXPECTED);
      });
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionOnReadOnlyViolatedException",
  async () => {
    const cluster = await buildCluster();
    try {
      const key = newKey("grain");
      const grain = cluster.getGrain(FaultTestGrainInterface, key);
      const coordinator = cluster.getGrain(FaultCoordinatorGrainInterface, newKey("coord"));

      await coordinator.multiGrainSet([key], EXPECTED);
      await expect(coordinator.updateViolated(key, EXPECTED)).rejects.toThrow(
        TransactionReadOnlyViolatedError,
      );

      await testAfterDustSettles(async () => {
        const actual = await grain.get();
        for (const value of actual) expect(value).toBe(EXPECTED);
      });
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.MultiGrainAbortTransactionOnExceptions",
  async () => {
    const cluster = await buildCluster();
    try {
      const grainCount = MAX_COORDINATED_TRANSACTIONS - 1;
      const throwKey = newKey("throw");
      const throwGrain = cluster.getGrain(FaultTestGrainInterface, throwKey);
      const keys = Array.from({ length: grainCount }, () => newKey("grain"));
      const coordinator = cluster.getGrain(FaultCoordinatorGrainInterface, newKey("coord"));

      await throwGrain.set(EXPECTED);
      await coordinator.multiGrainSet(keys, EXPECTED);
      await expect(coordinator.multiGrainAddAndThrow([throwKey], keys, EXPECTED)).rejects.toThrow(
        TransactionAbortedError,
      );

      const allKeys = [...keys, throwKey];
      await testAfterDustSettles(async () => {
        for (const key of allKeys) {
          const actual = await cluster.getGrain(FaultTestGrainInterface, key).get();
          for (const value of actual) expect(value).toBe(EXPECTED);
        }
      });
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionExceptionInnerExceptionOnlyContainsOneRootCauseException",
  async () => {
    const cluster = await buildCluster();
    try {
      const throwGrainCount = 3;
      const grainCount = MAX_COORDINATED_TRANSACTIONS - throwGrainCount;
      const throwKeys = Array.from({ length: throwGrainCount }, () => newKey("throw"));
      const keys = Array.from({ length: grainCount }, () => newKey("grain"));
      const coordinator = cluster.getGrain(FaultCoordinatorGrainInterface, newKey("coord"));

      await coordinator.multiGrainSet(throwKeys, EXPECTED);
      await coordinator.multiGrainSet(keys, EXPECTED);

      const innerExceptionCheck = async (): Promise<void> => {
        try {
          await coordinator.multiGrainAddAndThrow(throwKeys, keys, EXPECTED);
        } catch (error) {
          expect(error).toBeInstanceOf(TransactionAbortedError);
          expect((error as TransactionAbortedError).cause).toBeInstanceOf(AddAndThrowError);
          throw error;
        }
      };

      await expect(innerExceptionCheck()).rejects.toThrow(TransactionAbortedError);

      const allKeys = [...keys, ...throwKeys];
      await testAfterDustSettles(async () => {
        for (const key of allKeys) {
          const actual = await cluster.getGrain(FaultTestGrainInterface, key).get();
          for (const value of actual) expect(value).toBe(EXPECTED);
        }
      });
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "Orleans.Transactions.Tests.GrainFaultTransactionMemoryTests.AbortTransactionOnOrphanCalls",
  async () => {
    const cluster = await buildCluster();
    try {
      const key = newKey("grain");
      const grain = cluster.getGrain(FaultTestGrainInterface, key);
      const coordinator = cluster.getGrain(FaultCoordinatorGrainInterface, newKey("coord"));

      await grain.set(EXPECTED);
      await expect(coordinator.orphanCallTransaction()).rejects.toThrow(TransactionOrphanCallError);

      await testAfterDustSettles(async () => {
        const actual = await grain.get();
        for (const value of actual) expect(value).toBe(EXPECTED);
      });
    } finally {
      await cluster.dispose();
    }
  },
);
