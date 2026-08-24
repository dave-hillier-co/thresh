// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Disabled/DisabledTransactionsTests.cs @ v10.1.0 (MIT).
//
// (The assignment also names Memory/DisabledTransactionsTests.cs, but no such
// file exists at v10.1.0 — only Disabled/DisabledTransactionsTests.cs does.)
//
// DisabledTransactionsTests uses DefaultClusterFixture, whose SiloHostConfigurator
// never calls `.UseTransactions()`, so any grain call carrying a `[Transaction]`
// attribute throws OrleansTransactionsDisabledException. It inherits both its
// [Theory] methods, undecorated by any Skip, from DisabledTransactionsTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/DisabledTransactionsTestRunner.cs):
// TransactionGrainsThrowWhenTransactions calls a single `[Transaction(Create)]`
// grain method directly; MultiTransactionGrainsThrowWhenTransactions routes the
// same assertion through a coordinator grain whose own `[Transaction(Create)]`
// method fans out `Set`/`Add` calls (`[Transaction(Join)]`) to several grains.
//
// `TestCluster` now supports `{ transactions: false }` (@thresh/testing), which
// builds every silo via `SiloBuilder.disableTransactions()` — no transactional
// storage provider is wired, so `GrainFactory.resolveTransaction` throws
// `TransactionsDisabledError` (@thresh/core/errors) before the call ever reaches
// an activation.
import { expect } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { TransactionsDisabledError } from "@thresh/core/errors";
import { TestCluster } from "@thresh/testing/test-cluster";
import { orleansTest } from "@thresh/testing/orleans-test";

interface Counter {
  value: number;
}

interface TransactionTestGrain extends GrainKey<string> {
  set(delta: number): Promise<void>;
  add(delta: number): Promise<void>;
  get(): Promise<number>;
}

interface TransactionCoordinatorGrain extends GrainKey<string> {
  multiGrainSet(keys: string[], delta: number): Promise<void>;
}

const TransactionTestGrainInterface = defineGrainInterface<TransactionTestGrain>(
  "DisabledTxTestGrain",
  {
    options: {
      set: { transaction: "create" },
      add: { transaction: "join" },
      get: { transaction: "createOrJoin" },
    },
  },
);

const TransactionCoordinatorGrainInterface = defineGrainInterface<TransactionCoordinatorGrain>(
  "DisabledTxCoordinatorGrain",
  { options: { multiGrainSet: { transaction: "create" } } },
);

const TransactionTestGrainImpl = defineGrain<TransactionTestGrain>("DisabledTxTestGrain", () => {
  const counter = useTransactionalState<Counter>("counter", {
    initial: () => ({ value: 0 }),
  });
  return {
    set: (delta) =>
      counter.performUpdate((s) => {
        s.value = delta;
      }),
    add: (delta) =>
      counter.performUpdate((s) => {
        s.value += delta;
      }),
    get: () => counter.performRead((s) => s.value),
  };
});

const TransactionCoordinatorGrainImpl = defineGrain<TransactionCoordinatorGrain>(
  "DisabledTxCoordinatorGrain",
  (ctx) => ({
    multiGrainSet: async (keys, delta) => {
      for (const key of keys) {
        await ctx.getGrain(TransactionTestGrainInterface, key).add(delta);
      }
    },
  }),
);

async function buildDisabledTransactionsCluster(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    transactions: false,
    grains: [
      { ctor: TransactionTestGrainImpl.grain, interfaces: [TransactionTestGrainInterface] },
      {
        ctor: TransactionCoordinatorGrainImpl.grain,
        interfaces: [TransactionCoordinatorGrainInterface],
      },
    ],
  });
}

orleansTest(
  "Orleans.Transactions.Tests.DisabledTransactionsTests.TransactionGrainsThrowWhenTransactions",
  async () => {
    const cluster = await buildDisabledTransactionsCluster();
    try {
      const grain = cluster.getGrain(TransactionTestGrainInterface, "grain-0");
      await expect(grain.set(5)).rejects.toThrow(TransactionsDisabledError);
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "Orleans.Transactions.Tests.DisabledTransactionsTests.MultiTransactionGrainsThrowWhenTransactions",
  async () => {
    const cluster = await buildDisabledTransactionsCluster();
    try {
      const keys = ["grain-0", "grain-1", "grain-2"];
      const coordinator = cluster.getGrain(TransactionCoordinatorGrainInterface, "coordinator-0");
      await expect(coordinator.multiGrainSet(keys, 5)).rejects.toThrow(TransactionsDisabledError);
    } finally {
      await cluster.dispose();
    }
  },
);
